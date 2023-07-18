from overrides import override

import queue
from queue import SimpleQueue
from threading import Event
from selectors import EVENT_READ
import os
import logging
from traceback import print_exc

from serve_common.ipc.sockutil import *
from serve_common.ipc.selectors import *
from serve_common.compute.executor import *
from serve_common.threads import thread_target
from serve_common.export import *

logger = logging.getLogger(__name__)

__all__ = [
    "PoolWorker",
    "run_process_pool"
]


@export
class PoolWorker:
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


class PoolWorkerManager(ExecutorWorker):
    """
    A worker that manages a process pool of sub-workers.

    There are three "actors" at play during the operation of this class:
    the ExecutorServer, the manager (this class), and the pool worker
    (not to be confused with ExecutorWorker).
    The server manages connections with clients and job tracking as usual,
    within which the submitted tasks are assigned to this manager
    (an instance of ExecutorWorker),
    who further delegates them to the workers in the process pool it manages.
    """

    def __init__(self, pool_size: int, worker: PoolWorker):
        super().__init__()
        self.pool_size = pool_size
        self.worker = worker
        self.shutdown_event = Event()


    @override
    def setup(self, send_result):
        logger.debug("setting up worker pool")

        self.send_result = send_result

        # idle_workers are populated as workers are forked.
        # Whenever jobs are assigned to us (added to pending_jobs),
        # an idle worker is removed from the queue
        # and the job delegated to it.
        self.idle_workers = SimpleQueue()  # worker sockets
        self.pending_jobs = SimpleQueue()  # Job
        self.active_jobs = {}  # worker -> Job

        # The scheduler waits for a worker and a job.
        # When both are ready, it sends the job to the worker.
        @thread_target
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
        self.scheduler = scheduler

        # prep and fork workers
        logger.debug("preparing workers")
        self.worker.pre_fork()
        for _ in range(self.pool_size):
            self.start_worker()
        logger.debug("worker pool populated")

        self.scheduler.start()

        # The selector is responsible for awaiting the results from workers.
        self.selector_loop = SelectorRunner(do_callback_on_ready)
        self.selector_loop.start()

        logger.info("worker pool ready")


    @override
    def assign(self, job: Job):
        self.pending_jobs.put(job)


    def register_worker(self, fd):
        self.idle_workers.put(fd)
        self.selector_loop.register(fd, EVENT_READ, self._on_completion)

    def unregister_worker(self, fd):
        self.selector_loop.unregister(fd)
        fd.close()


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
                    # Payload of Job, can by any type.
                    # This allows specialized workers to accept only arguments.
                except EOFError:
                    # shutdown
                    break

                error, result = run_safely(lambda: self.worker.run_task(task))
                result_type = Message.Type.ERROR if error \
                        else Message.Type.DATA
                send(sc, Message(result_type, result))
        except:
            print_exc()
            sc.close()
            os._exit(1)
        sc.close()
        os._exit(0)


    def _on_completion(self, worker):
        """
        Invoked when a worker has produced a result.
        """
        try:
            answer = recv(worker)
        except OSError:
            # should never happen
            print_exc()
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
        self.send_result(Result(
                job.id,
                answer.data,
                answer.type == Message.Type.DATA))


    @override
    def shutdown(self):
        self.selector_loop.stop()
        self.shutdown_event.set()
        self.scheduler.join()


@export
def run_process_pool(pool_id: str, pool_size: int, worker: PoolWorker = None):
    """
    Starts an ExecutorServer backed by a process pool
    and runs it until shutdown.

    This is usually used in combination with compute.spawn_process.
    """
    run_executor(ExecutorServer, pool_id, PoolWorkerManager(pool_size, worker))
