from typing import Callable, Union, Literal
import time
from functools import wraps, partial
import logging
from logging.handlers import SocketHandler
import socket
from selectors import EVENT_READ, BaseSelector, DefaultSelector
import struct
import pickle
from queue import SimpleQueue
from threading import Condition

from overrides import override

from serve_common import request_id
from serve_common.threads import *
from serve_common.utils import as_context
from serve_common.ipc import sockutil
from serve_common.ipc.server import Rendezvous


logger = logging.getLogger(__name__)


class RequestIdInjector:  # Quacks like a logging.Filter.
    def filter(self, record) -> Literal[1]:
        already_set = False
        try:
            if record.request_id:
                already_set = True
        except AttributeError:
            pass
        if not already_set:
            rid = request_id.get()
            record.request_id = f"[{rid}] " if rid is not None else ""
        return 1


class RequestTimer:
    """Falcon middleware to time each request."""

    def process_resource(self, req, resp, res, params):
        logger.info(f"hit {req.path}")
        req.context.start_time = time.monotonic_ns()

    def process_response(self, req, resp, res, req_succeeded):
        # unlike process_resource, this is called even if no route matched
        if res is None:
            return
        duration = (time.monotonic_ns() - req.context.start_time) / 1e6
        logger.info(f"request completed with {resp.status} (took {duration:.2f} ms)")


class time_and_log:
    """
    A decorator and context manager to time and log a block or function.

    The message to log with the timing information can be supplied
    as a constant string or a callable that returns a string.
    In the latter case, the callable will be invoked with no arguments
    when used as a context manager,
    and with all arguments to the decorated function when used as a decorator.
    """

    def __init__(self, msg: Union[str, Callable[[...], str]]):
        self.msg = msg
        self.start_time = None

    def __enter__(self):
        self.start_time = time.monotonic_ns()

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.monotonic_ns() - self.start_time
        msg = self.msg() if callable(self.msg) else self.msg
        logger.info(f"{msg}: {duration} ns")
        return False

    def __call__(self, func):
        @wraps(func)
        def wrap(*args, **kwargs):
            if callable(self.msg):
                context = time_and_log(partial(self.msg, *args, **kwargs))
            else:
                context = time_and_log(self.msg)
            with context:
                return func(*args, **kwargs)
        return wrap


def catch(
        func=None,
        *,
        logger: str = None,
        level=logging.ERROR,
        reraise=False,
        msg="Error caught"):
    """
    Decorates the function to catch and log any exceptions,
    then optionally reraise them.
    The exception will be logged to the specified logger and level,
    or to the logger named by the module name of the function, if not specified.
    The log entry will begin with the provided message,
    then list the stack trace in the standard format produced by Python.
    """

    def dec(func):
        name = logger if logger is not None else func.__module__

        @wraps(func)
        def wrap(*args, **kws):
            try:
                return func(*args, **kws)
            except:
                logging.getLogger(name).log(level, msg, exc_info=True)
                if reraise:
                    raise
        return wrap
    if func:
        return dec(func)
    return dec


class LogServerHandler(logging.Handler):
    """
    A handler that connects to the LogServer identified by the id,
    and delegates all logging to the server.
    """

    def __init__(self, server_id: str, level=logging.NOTSET):
        super().__init__(level)
        self.server_id = server_id
        self.sock_handler = None
        self._lock_context = as_context(self.acquire, self.release)
        # internally it's an RLock


    @sync_method("_lock_context")
    def emit(self, record):
        if self.sock_handler is None:
            try:
                address = LogServer.request_address(self.server_id)
            except:
                from serve_common._debug import pdebug
                from serve_common.utils import repr_call
                pdebug("FAIL:", repr_call(LogServer.request_address, self.server_id))
                raise
            # TODO EOFError requesting address, how?
            # Fails in gunicorn worker and pool worker
            self.sock_handler = SocketHandler(address, None)
        try:
            self.sock_handler.emit(record)
        except:
            self.handleError(record)


    @sync_method("_lock_context")
    def handleError(self, record):
        if self.sock_handler:
            self.sock_handler.close()
        self.sock_handler = None


    @sync_method("_lock_context")
    def close(self):
        if self.sock_handler:
            self.sock_handler.close()
        super().close()


class LogServer(Rendezvous):
    """
    Centralized logging server.
    Log messages are sent to and logged by this class.
    This ensures that
    a) logging from multiple processes to the same file is safe, and
    b) logging configuration does not need to be shared to sub-processes.
    """

    # Format used by SocketHandler.
    _HEADER_FMT = ">L"


    # TODO busy loop somewhere in logging process

    def __init__(self,
            server_id: str,
            selector_factory: Callable[[], BaseSelector] = DefaultSelector,
            listener_factory: Callable[[], socket] = partial(
                    sockutil.create_listener, sktype=socket.SOCK_STREAM)):
        super().__init__(server_id, selector_factory, listener_factory)

        self.writer_control = SimpleQueue()

        @thread_target
        def record_writer():
            while True:
                match self.writer_control.get():
                    case ("LOG", record):
                        try:
                            logger = logging.getLogger(record.name)
                            if record.levelno >= logger.getEffectiveLevel():
                                logger.handle(record)
                        except:
                            # handle already handles errors,
                            # so this only happens when logger went really bad.
                            import traceback
                            traceback.print_exc()
                    case ("STOP",):
                        # When the STOP is put in the queue,
                        # listener and selector have already stopped,
                        # and no more logs could be added.
                        # Hence, this STOP is guaranteed to be the last command,
                        # which ensures all queued logs are written.
                        break
        self.record_writer = record_writer


    @override
    def start(self):
        super().start()
        self.record_writer.start()


    @override
    def accept(self, listener):
        client = super().accept(listener)
        self.selector_loop.register(client, EVENT_READ, self.recv_log)


    def recv_log(self, client):
        # Receive a log record;
        # a LogServerHandler is on the other end of the socket.
        # recv header
        header = sockutil.recv_all(client, struct.calcsize(self._HEADER_FMT))
        # extract message length from header
        data_len = struct.unpack(self._HEADER_FMT, header)[0]
        # recv message body
        data = sockutil.recv_all(client, data_len)
        # parse and reconstruct into LogRecord
        record = logging.makeLogRecord(pickle.loads(data))
        # queue it to be written
        self.writer_control.put(("LOG", record))


    @override
    def shutdown(self):
        super().shutdown()
        self.writer_control.put(("STOP",))

    @override
    def await_shutdown(self):
        self.shutdown_event.wait()
        self.record_writer.join()
