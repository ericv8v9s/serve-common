from typing import Callable, Any
from socket import socket
from overrides import override

import queue
from queue import SimpleQueue
from concurrent.futures import Executor, Future
from threading import RLock, Event, Condition, Thread
from selectors import EVENT_READ, DefaultSelector
import pickle
from functools import partial
from dataclasses import dataclass
from enum import IntEnum, auto
import os

from serve_common import ipc
from serve_common.ipc.ipc import AbstractSocketServer
from serve_common import compute
from serve_common.ipc.sockutil import *
from serve_common.export import *


@export
def identity(*args, **kwargs):
    """
    Returns all arguments unchanged as a tuple of (args, kwargs).
    Useful for submitting tasks to workers that
    have the actual computation function defined internally.
    """
    return args, kwargs


def _connect_pool_manager(pool_id: str, timeout: float=None) -> socket:
    from time import sleep, monotonic
    if timeout is not None:
        end = monotonic() + timeout

    retries = 3
    retries_left = 0
    delay = 10 / 1000  # 10 ms

    # The PoolManager's advertiser is always listening on this group
    # and replying any inquiries with the manager's address.
    address = None
    with ipc.join_group(f"{__name__}:{pool_id}") as group:
        group.setblocking(False)
        while timeout is None or monotonic() < end:
            if retries_left <= 0:
                send(group, "where")
                retries_left = retries
            try:
                address = recv(group)
                # address are bytes, commands are str
                if isinstance(address, bytes):
                    break
                else:
                    address = None
            except BlockingIOError:
                retries_left -= 1
                sleep(delay)

    if address is None:
        raise TimeoutError()

    return connect(address)


@dataclass
class Message:
    class Type(IntEnum):
        CONTROL = auto()
        ERROR   = auto()
        DATA    = auto()
    type: Type
    data: Any

    def __hash__(self):
        return hash((self.type, self.data))


@dataclass
class Job:
    id: int
    task: Callable

    def __hash__(self):
        return hash((self.id, self.task))


@export
class ProcessPoolClient(Executor):
    def __init__(self, pool_id: str):
        # connect to pool manager
        self.conn_lock = RLock()
        self.connection = _connect_pool_manager(pool_id)
        # first message will tell us how to generate job id's
        init_msg = recv(self.connection)
        if init_msg.type is Message.Type.ERROR:
            self.connection.close()
            raise RuntimeError(init_msg.data)

        self.job_id_lock = RLock()
        self.next_job_id, self.id_step = init_msg.data

        self.pool_id = pool_id
        self.shutdown_event = Event()

        self.jobs_lock = Condition()
        self.pending_jobs = {}  # job id -> Future

        def retrieve():
            while not self.shutdown_event.is_set():
                with self.jobs_lock:
                    # regularly checks for shutdown
                    if not self.jobs_lock.wait_for(
                            lambda: len(self.pending_jobs) > 0,
                            timeout=50/1000):
                        continue

                try:
                    resp = recv(self.connection)
                    is_error = resp.type == Message.Type.ERROR
                    job_id, result = resp.data

                    with self.jobs_lock:
                        future = self.pending_jobs.pop(job_id)
                    if is_error:
                        future.set_exception(
                                RuntimeError(f"Error in worker:\n{result}"))
                    else:
                        future.set_result(result)
                except EOFError:
                    if self.shutdown_event.is_set():
                        break
                    import traceback
                    traceback.print_exc()
                except Exception:
                    import traceback
                    traceback.print_exc()
        self.retriever = Thread(target=retrieve, daemon=True)
        self.retriever.start()


    def gen_next_job_id(self) -> int:
        with self.job_id_lock:
            job_id = self.next_job_id
            self.next_job_id += self.id_step
            return job_id


    @override
    def submit(self, fn, /, *args, **kwargs) -> Future:
        if self.shutdown_event.is_set():
            raise RuntimeError("executor has shutdown")

        job_id = self.gen_next_job_id()
        data = pickle.dumps(Message(
                Message.Type.DATA,
                Job(job_id, partial(fn, *args, **kwargs))))

        future = Future()
        # we do not support cancellation
        future.set_running_or_notify_cancel()

        with self.jobs_lock:
            self.pending_jobs[job_id] = future
            self.jobs_lock.notify()
        with self.conn_lock:
            send_raw(self.connection, data)
        return future


    @override
    def shutdown(self, wait=True, *, cancel_futures=False):
        self.shutdown_event.set()
        self.connection.close()
        if wait:
            self.retriever.join()


@export
class PoolManager(AbstractSocketServer):
    MAX_CLIENTS = 256
    """Maximum total number of clients (current and disconnected)."""
    # This is to ensure every client can generate unique job id's.


    def __init__(self, pool_id: str, pool_size: int, worker: "Worker"):
        super().__init__(DefaultSelector, create_listener)
        self.pool_id = pool_id
        self.pool_size = pool_size
        self.worker = worker

        # Advertise the server's address to anyone asking in the group.
        self.advert_group = ipc.join_group(f"{__name__}:{pool_id}")
        self.selector.register(self.advert_group, EVENT_READ, self._on_inquiry)

        self.client_counter = 0
        self.clients = set()  # connected client sockets
        self.waiting_clients = {}  # Job -> client

        self.idle_workers = SimpleQueue()  # worker sockets
        self.pending_jobs = SimpleQueue()  # Job
        self.active_jobs = {}  # worker -> Job

        def scheduler():
            worker = job = None
            while not self.shutdown_event.is_set():
                try:
                    if worker is None:
                        worker = self.idle_workers.get(timeout=50/1000)
                    if job is None:
                        job = self.pending_jobs.get(timeout=50/1000)
                except queue.Empty:
                    # regularly checks exit condition
                    continue

                self.active_jobs[worker] = job
                send(worker, job.task)

                worker = job = None
        self.scheduler = Thread(target=scheduler, daemon=True)


    def add_client(self, fd):
        self.client_counter += 1
        self.clients.add(fd)
        self.selector.register(fd, EVENT_READ, self._on_submit)

    def remove_client(self, fd):
        self.clients.remove(fd)
        self.selector.unregister(fd)
        fd.close()


    def register_worker(self, fd):
        self.idle_workers.put(fd)
        self.selector.register(fd, EVENT_READ, self._on_completion)

    def unregister_worker(self, fd):
        self.selector.unregister(fd)
        fd.close()


    def _on_inquiry(self, group):
        try:
            msg = recv(group)
        except EOFError:
            return
        if msg == "where":
            send(group, self.listener.getsockname())


    @override
    def accept(self, listener):
        # A new client has connected.
        # We try to assign a "client id" to it,
        # which is used by the client to generate job id's.
        # If we ran out of client id's, we reject the client.

        client = None
        try:
            client, _ = listener.accept()

            if self.client_counter >= self.MAX_CLIENTS:
                send(client, Message(
                        Message.Type.ERROR,
                       f"Exceeding maximum number of clients ({self.MAX_CLIENTS})"))
                client.close()
                return

            send(client, Message(
                    Message.Type.DATA,
                    (self.client_counter, self.MAX_CLIENTS)))
            self.add_client(client)
        except OSError:
            if client is not None:
                client.close()


    def _on_submit(self, client):
        # The client is submitting a task.
        try:
            job = recv(client).data
            self.waiting_clients[job] = client
            self.pending_jobs.put(job)
        except (OSError, EOFError):
            self.remove_client(client)
            return


    def _on_completion(self, worker):
        try:
            answer = recv(worker)
        except OSError:
            # should never happen
            import traceback
            traceback.print_exc()
            self.unregister_worker(worker)
            return
        except EOFError:
            # peer closed => abnormal worker shutdown
            # (normal shutdown won't trigger this call at all)
            self.unregister_worker(worker)
            return

        # return worker back for more jobs
        self.idle_workers.put(worker)

        job = self.active_jobs.pop(worker)
        client = self.waiting_clients.pop(job)

        try:
            send(client, Message(answer.type, (job.id, answer.data)))
        except OSError:
            self.remove_client(client)


    def start_worker(self):
        sp, sc = socketpair()

        if os.fork():
            sc.close()
            self.register_worker(sp)
            return

        # in child
        try:
            sp.close()
            self.worker.post_fork()

            while True:
                try:
                    task = recv(sc)
                except EOFError:
                    # shutdown
                    break

                result_type = Message.Type.DATA
                try:
                    result = self.worker.run_task(task)
                except Exception as e:
                    result = e
                    result_type = Message.Type.ERROR
                send(sc, Message(result_type, result))
        except:
            sc.close()
            os._exit(1)
        sc.close()
        os._exit(0)


    @override
    def start(self):
        self.worker.pre_fork()
        for _ in range(self.pool_size):
            self.start_worker()

        super().start()
        self.scheduler.start()


    @override
    def shutdown(self):
        super().shutdown()
        self.advert_group.close()
        self.scheduler.join()


@export
class Worker:
    """
    Base worker that does no initialization
    and simply runs each task and returns the result.
    """
    def pre_fork(self):
        pass

    def post_fork(self):
        pass

    def run_task(self, task):
        return task()


@export
def process_pool(pool_id: str, pool_size: int, worker: Worker = None):
    if ipc.ipc.server_address is None:
        raise RuntimeError("IPC not setup (forgot to call ipc.setup()?)")
    if worker is None:
        worker = Worker()
    manager = PoolManager(pool_id, pool_size, worker)
    manager.start()
    compute.event_shutdown.wait()
    manager.shutdown()
