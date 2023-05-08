import time
from functools import wraps, partial
from typing import Callable, Union

from loguru import logger

from serve_common.export import *
from serve_common.config import config
from serve_common import callback


@export
class LogLevelFilter:
    """A loguru filter to allow changing log level on a sink."""

    def __init__(self, config_path):
        level = config.get(config_path, default="INFO")

        self._level = "INFO"
        self.level = level

        # allow changing log level at runtime
        def change_log_level(_, old_level, new_level):
            logger.info(f"setting log level to [{new_level}]")
            self.level = new_level
        callback.register("config:logging.level", change_log_level)

    @property
    def level(self):
        return self._level

    @level.setter
    def level(self, level: str):
        try:
            logger.level(level)
        except ValueError:
            logger.error(f"log level [{level}] does not exist,"
                         f" remaining at [{self.level}]")
            return
        self._level = level

    def __call__(self, record) -> bool:
        return record["level"].no >= logger.level(self.level).no


@export
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


@export
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

