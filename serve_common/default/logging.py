import sys
import os
from loguru import logger
from serve_common import request_id
from serve_common.config import config
from serve_common.log_util import *


llfilter = LogLevelFilter("logging.level")


def format_msg(record) -> str:
    time = "[{time:YYYY-MM-DD HH:mm:ss.SSS}] "
    level = "[{level: <8}] "
    pid = "[pid {process}] "

    location = ""
    curr_level = logger.level(llfilter.level).no
    thresh_level = logger.level("TRACE").no
    if curr_level <= thresh_level:
        location = "[{name}:{function}:{line}] "

    rid = request_id.get()
    rid_fmt = ""
    if rid is not None:
        rid_fmt = f"[{rid}] "

    return (
        "<level>"
       f"{time}{level}{pid}{location}{rid_fmt}"
        "{message}"
        "</level>\n{exception}")


def add_output_stderr():
    # remove default logger output
    logger.remove(0)

    logger.add(sys.stderr,
        level=0,
        format=format_msg,
        filter=llfilter,
        enqueue=True)


def add_output_file(filename="app.log"):
    log_dir = config.get("logging.logs_dir", default="logs")
    os.makedirs(log_dir, mode=0o700, exist_ok=True)
    logger.add(f"{log_dir}/{filename}",
        level=0,
        format=format_msg,
        filter=llfilter,
        rotation="2 MB",
        enqueue=True)

