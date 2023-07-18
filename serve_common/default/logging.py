"""
Default logging setup.

The default configuration after calling setup contains a formatter
(using ``format`` defined in this module);
a filter that injects request_id's into the log message if available;
and a handler that logs to stderr all messages of DEBUG and above,
applying the aformentioned formatter and filter;
and 2 loggers, both logging at configurable level
and one is named as specified by the call to ``setup``.
The other logger has the name ``serve_common``,
and is the parent of all loggers used in this library.
"""

from typing import Union
import logging


format = (
    "[{asctime}] [{levelname:<8}] [{processName}({process})] {request_id}"
    "{name}: {message}")
datefmt = "%Y-%m-%d %H:%M:%S %z"


# https://docs.python.org/3/library/logging.config.html#logging-config-dictschema
def get_default_config():
    return {
        "version": 1,
        "formatters": {
            "serve_common_default_formatter": {
                "format": format,
                "datefmt": datefmt,
                "style": "{"
            }
        },
        # A filter that injects request id's.
        "filters": {
            "request_id_injector": {
                "()": "serve_common.logging.RequestIdInjector"
            }
        },
        # Log to stderr.
        "handlers": {
            "serve_common_default_handler": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "serve_common_default_formatter",
                "filters": ["request_id_injector"]
            }
        },
        "disable_existing_loggers": False
    }


def setup_loggers(loggers: dict):
    """
    Sets up a default logging configuration (see source for config dict).
    The provided loggers dict is inserted as the value of the "loggers" key
    of the configuration dict after processing.

    The provided loggers dict goes through the following processing:
    first, a default configuration for the "serve_common" logger is added
    if said key is absent;
    next, each logger is added a default handler that logs to stderr
    if no "handlers" is specified;
    lastly, loggers with the value None are removed
    (this can be used to disable default loggers
    e.g. mapping "serve_common" to None).
    """

    import logging.config

    # add default serve_common logger
    if "serve_common" not in loggers:
        loggers["serve_common"] = {}

    # insert default handlers
    for logger in loggers.values():
        if logger is None:
            continue
        if "handlers" not in logger:
            logger["handlers"] = ["serve_common_default_handler"]

    # prune None's
    for k, v in list(loggers.items()):
        if v is None:
            del loggers[k]

    config = get_default_config()
    config["loggers"] = loggers

    logging.config.dictConfig(config)
