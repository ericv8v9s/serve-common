from serve_common.export import *

import time
from functools import wraps
from typing import Callable, Union

from loguru import logger
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
        callback.register("logging.level", change_log_level)

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
def time_and_log(msg: Union[str, Callable[[...], str]]):
    def decorator(func):
        if callable(msg):
            @wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.monotonic_ns()
                ret_val = func(*args, **kwargs)
                logger.debug(f"{msg(*args, **kwargs)}: {time.monotonic_ns() - start_time} ns")
                return ret_val
        else:
            @wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.monotonic_ns()
                ret_val = func(*args, **kwargs)
                logger.debug(f"{msg}: {time.monotonic_ns() - start_time} ns")
                return ret_val
        return wrapper
    return decorator
