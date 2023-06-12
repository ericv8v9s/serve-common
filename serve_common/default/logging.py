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
    "[%(asctime)s] [%(process)d] [%(levelname)s] %(request_id)s"
    "%(name)s: %(message)s")


# https://docs.python.org/3/library/logging.config.html#logging-config-dictschema
def setup(*,
        app_name="app",
        level: Union[int, str] = logging.INFO):
    """
    Sets up a default logging configuration (see source for config dict).
    The app_name is used as a logger name;
    commonly this should be the root module name of your project.
    """

    import logging.config

    config = {
        "version": 1,
        "formatters": {
            "serve_common_default_formatter": {
                "format": format,
                "datefmt": "%Y-%m-%d %H:%M:%S %z"
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
        "loggers": {
            app_name: {
                "level": level,
                "handlers": ["serve_common_default_handler"]
            },
            "serve_common": {
                "level": level,
                "handlers": ["serve_common_default_handler"]
            }
        },
        "disable_existing_loggers": False
    }
    logging.config.dictConfig(config)

