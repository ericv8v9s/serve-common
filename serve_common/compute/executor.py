"""
IPC based executor:
clients implement the Executor interface and connect to the ExecutorServer.
The server runs the tasks submitted by clients and sends the results back.
"""

from typing import Callable, Any, Generic, TypeVar, Union

from threading import Thread, RLock, Condition, Event
import queue
from queue import SimpleQueue
from selectors import EVENT_READ
from socket import socket
from concurrent.futures import Executor, Future
from dataclasses import dataclass
from enum import IntEnum, auto
from abc import ABC, abstractmethod
import pickle
from functools import partial

from overrides import override

from serve_common.export import *
from serve_common import compute
from serve_common import ipc
from serve_common.ipc.server import Rendezvous
from serve_common.ipc.sockutil import *

__all__ = [
    "Message",
    "Job",
    "Result",
    "run_safely",
    "ExecutorWorker",
    "ExecutorServer",
    "ExecutorClient",
    "run_executor"
]

import logging
logger = logging.getLogger(__name__)


@export
@dataclass(frozen=True)
class Message:
    """
    Represents a datagram used for communication
    between executor server/clients.
    """
    class Type(IntEnum):
        CONTROL = auto()
        ERROR   = auto()
        DATA    = auto()
    type: Type
    data: Any


# TODO inline this when 3.12 is out (PEP 695)
T = TypeVar("T", covariant=True)

@export
@dataclass(frozen=True)
class Job(Generic[T]):
    """A single unit of computation submitted by client to server."""

    id: int
    """
    Job id, unique among all jobs created from clients
    connected to the same server.
    """

    task: T
    """
    Opaque data representing the computation.
    This is a zero-parameter callable in most cases,
    but can be arbitrary objects for specialized workers.
    """


@export
@dataclass(frozen=True)
class Result(Generic[T]):
    """Result of a Job."""

    id: int
    """Id of the job this object is the result of."""
    data: T
    """Arbitrary result object."""
    error: bool = False
    """
    If True, an exception was raised and data contains the traceback string.
    """


R = TypeVar("R", covariant=True)
@export
def run_safely(task: Callable[[], R]) -> tuple[bool, Union[R, str]]:
    """
    Runs the task and returns the result or the stack trace string,
    if an exception occurred while executing the task
    (in which case the first item on the returned tuple is True,
    otherwise the value is False for normal return).
    """
    try:
        return (False, task())
    except Exception:
        import traceback
        return (True, traceback.format_exc())


P = TypeVar("P", contravariant=True)
@export
class ExecutorWorker(Generic[P, R], ABC):
    """
    A worker object that implements the execution of jobs in ExecutorServers.
    """

    @staticmethod
    def run_job_safely(job: Job[Callable[[], R]]) -> Result[Union[R, str]]:
        """
        Runs ``job.task`` and wraps the return value
        or raised exception stack trace string in a Result object.
        """
        error, result = run_safely(job.task)
        return Result(job.id, result, error)


    @abstractmethod
    def setup(self, send_result: Callable[[Result[R]], None]):
        """
        Sets up this worker.
        This function will be invoked during the ExecutorServer init.
        The argument send_result will be a function provided by the server
        with which results can be sent back.

        This is useful when the worker should be initialized
        in a different context than that of the call site of its init
        (e.g. when the ExecutorServer is running in a separate process).
        """
        pass

    @abstractmethod
    def assign(self, job: Job[P]):
        """
        Assigns a job to this worker.
        It is up to the implementation to decide how the job will be run.
        """
        pass

    def shutdown(self):
        """
        Requests this worker to cleanup and shutdown.
        """
        pass


class SingleThreadWorker(ExecutorWorker):
    """
    A worker that runs each job sequentially.
    This worker requires tasks to be no-parameter callables.
    """

    @override
    def setup(self, send_result):
        self.send_result = send_result
        self.jobs = SimpleQueue()
        def run_tasks():
            while True:
                job = self.jobs.get()
                self.send_result(self.run_job_safely(job))
        self.runner = Thread(target=run_tasks, daemon=True)
        self.runner.start()

    @override
    def assign(self, job):
        self.jobs.put(job)


@export
class ExecutorServer(Rendezvous):
    """
    A server that allows clients to connect and submit tasks,
    which will then be executed by its worker and the results sent back.
    """

    @dataclass
    class Client:
        id: int
        socket: socket
        lock: RLock

    MAX_CLIENTS = 256  # TODO somehow remove this limit
    """Maximum number of concurrent clients."""
    # This is to ensure every client can generate unique job id's.

    def __init__(self, server_id: str, worker: ExecutorWorker):
        super().__init__(server_id)

        worker.setup(self.on_receive_result)
        self.worker = worker

        self.clients = {}  # client socket -> client

        # Guards client id tracking.
        self.clients_lock = RLock()
        # id's that are free to assign to clients
        # removed as clients connect, put back as clients disconnect
        self.free_client_ids = SimpleQueue()
        for i in range(self.MAX_CLIENTS):
            self.free_client_ids.put(i)

        self.running_jobs_lock = RLock()
        # clients waiting for result
        self.waiting_clients = {}  # job id -> client


    def add_client(self, sock: socket) -> Client:
        """
        Adds a connected client; returns the assigned id.
        If no more id's available, raises queue.Empty.
        """
        with self.clients_lock:
            client_id = self.free_client_ids.get_nowait()
            client = ExecutorServer.Client(client_id, sock, RLock())
            self.selector_loop.register(sock, EVENT_READ, self.on_submit)
            return client

    def remove_client(self, client: Union[Client, socket]):
        """Disconnects a client."""
        try:
            with self.clients_lock:
                if isinstance(client, socket):
                    client = self.clients[client]
                self.selector_loop.unregister(client.socket)
                del self.clients[client.socket]
                self.free_client_ids.put(client.id)
                client.socket.close()
        except KeyError:
            # Why would this happen?
            # We disconnect clients whenever we detect the socket is dead.
            # Since each client may submit multiple jobs,
            # we may have running jobs from dead clients.
            # When the first of such a job completes,
            # this situation is detected and the client is removed;
            # subsequent jobs will now find that the submitting client
            # no longer exists.
            pass


    @override
    def accept(self, listener):
        sock = super().accept(listener)
        if sock is None:
            return

        try:
            client = self.add_client(sock)
            try:
                send(client.socket, Message(
                        Message.Type.DATA,
                        (client.id, self.MAX_CLIENTS)))
            except OSError:
                self.remove_client(client)

        except queue.Empty:
            # out of id's
            try:
                send(sock, Message(
                        Message.Type.ERROR,
                        f"Too many clients (max {self.MAX_CLIENTS})"))
            except OSError:
                pass
            sock.close()


    def on_submit(self, sock: socket):
        """
        Invoked when the client is trying to submit a task;
        data is available in the client socket
        and the next recv() on it returns a Message containing the Job.

        This function will be invoked as callback by the selector loop.
        """
        try:
            msg = recv(sock)
        except (OSError, EOFError):
            self.remove_client(sock)
            return

        if msg.type is Message.Type.DATA:
            job = msg.data
            with self.running_jobs_lock:
                self.waiting_clients[job.id] = self.clients[sock]
            self.worker.assign(job)


    def on_receive_result(self, result: Result[Any]):
        """
        Invoked by the worker to send back the result of a completed job.

        Note that this function will be invoked by the worker,
        NOT by the selector loop.
        Subclasses should take this into consideration to ensure thread-safety.
        """
        with self.running_jobs_lock:
            client = self.waiting_clients.pop(result.id)
        try:
            with client.lock:
                send(client.socket, result)
        except OSError:
            self.remove_client(client)


    @override
    def shutdown(self):
        super().shutdown()
        self.worker.shutdown()
        for c in self.clients:
            c.socket.close()


@export
class ExecutorClient(Executor):
    """
    An Executor that delegates all computation to an ExecutorServer.
    """

    def __init__(self, server_id: str, connect_timeout: float=None):
        super().__init__()

        # connect to server
        logger.info(f"connecting to '{server_id}'")
        self.connection = ExecutorServer.connect_server(
                server_id, connect_timeout)

        # first message will tell us how to generate job id's
        init_msg = recv(self.connection)
        if init_msg.type is Message.Type.ERROR:
            # server rejected us
            self.connection.close()
            raise RuntimeError(init_msg.data)

        # Job id's are generated from an initial value in set increments.
        # By use the same increment and different initial values,
        # all clients can generate unique job id's
        # without additional coordination.
        self.job_id_lock = RLock()
        self.next_job_id, self.id_step = init_msg.data

        self.shutdown_event = Event()

        self.jobs_lock = Condition()
        self.pending_jobs = {}  # job id -> Future

        def retrieve():
            from time import sleep

            def AWAIT_JOB():
                """
                Waits for a job to appear in pending_jobs,
                then steps to RECV_RESULT.
                Also makes sure to empty pending_jobs before shutting down.
                """
                with self.jobs_lock:
                    if self.shutdown_event.is_set() \
                            and len(self.pending_jobs) == 0:
                        return SHUTDOWN

                    if self.jobs_lock.wait_for(
                            lambda: len(self.pending_jobs) > 0,
                            timeout=50/1000):
                        # job submitted and pending
                        return RECV_RESULT
                    else:
                        # timed out
                        return AWAIT_JOB

            def RECV_RESULT():
                try:
                    resp = recv(self.connection)
                except EOFError:
                    logger.warning("Server closed connection, shutting down client")
                    return ABORT
                except Exception:
                    logger.exception("Error receiving result:")
                    logger.error("shutting down client due to IPC error")
                    return ABORT

                result: Result = resp.data

                with self.jobs_lock:
                    future = self.pending_jobs.pop(result.id)
                if result.error:
                    # result.data is formatted stack trace string on error
                    # (see run_safely)
                    future.set_exception(
                            RuntimeError(f"Error in worker:\n{result.data}"))
                else:
                    future.set_result(result.data)

                with self.jobs_lock:
                    if len(self.pending_jobs) == 0:
                        return AWAIT_JOB
                    else:
                        return RECV_RESULT

            def SHUTDOWN():
                self.shutdown(wait=False)
                self.connection.close()
                return None

            def ABORT():
                """Abnormal shutdown; an error occurred in RECV_RESULT."""
                SHUTDOWN()
                for f in self.pending_jobs.values():
                    f.set_exception(RuntimeError())
                self.pending_jobs.clear()
                return None

            state = AWAIT_JOB
            while state is not None:
                state = state()

        self.retriever = Thread(target=retrieve, daemon=True)
        self.retriever.start()


    def gen_next_job_id(self) -> int:
        with self.job_id_lock:
            job_id = self.next_job_id
            self.next_job_id += self.id_step
            return job_id


    def submit_data(self, data: Any) -> Future:
        """
        Submits arbitrary (picklable) data to the executor.
        """
        if self.shutdown_event.is_set():
            raise RuntimeError("executor has shutdown")

        job_id = self.gen_next_job_id()

        serialized = pickle.dumps(Message(
                Message.Type.DATA,
                Job(job_id, data)))

        future = Future()
        # we do not support cancellation
        future.set_running_or_notify_cancel()

        with self.jobs_lock:
            self.pending_jobs[job_id] = future
        send_raw(self.connection, serialized)
        return future


    @override
    def submit(self, fn, /, *args, **kwargs) -> Future:
        return self.submit_data(partial(fn, *args, **kwargs))


    @override
    def shutdown(self, wait=True, *, cancel_futures=False):
        self.shutdown_event.set()
        if wait:
            self.retriever.join()


@export
def run_executor(constructor: Callable[..., ExecutorServer], *args, **kws):
    """
    Starts an instance of the ExecutorServer returned by the factory
    and runs it until shutdown.

    This is usually used in combination with compute.spawn_process.
    """
    from serve_common.utils import repr_call
    logger.debug(
            "starting executor server: "
           f"{repr_call(constructor, *args, **kws)}")
    server = constructor(*args, **kws)
    server.start()
    logger.info()
    compute.event_shutdown.wait()
    server.shutdown()
