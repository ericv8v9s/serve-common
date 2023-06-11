from typing import Callable, Union, Literal
import time
from functools import wraps, partial
import logging

from serve_common import request_id


logger = logging.getLogger(__name__)


class RequestIdInjector:  # Quacks like a logging.Filter.
    def filter(self, record) -> Literal[1]:
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
        logger.debug(f"{msg}: {duration} ns")
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
        level=logging.ERROR,
        reraise=False,
        msg="Error caught"):
    def dec(func):
        @wraps(func)
        def wrap(*args, **kws):
            try:
                return func(*args, **kws)
            except Exception as e:
                logging.getLogger(func.__module__).log(level, exc_info=True)
                if reraise:
                    raise e from None
        return wrap
    if func:
        return dec(func)
    return dec
