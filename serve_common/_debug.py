"""
Handy debug tools.
"""


def pdebug(*args, **kws):
    """
    Like ``print``, but defaults to stderr and writes extra information.
    """
    import os, sys, threading
    kws.setdefault("file", sys.stderr)
    print(f"DEBUG [{os.getpid()}/{threading.get_native_id()}]", *args, **kws)


def trace_call(func):
    """
    Decorates function to print out its entry and exit,
    including arguments and return value.
    """
    import functools
    @functools.wraps(func)
    def wrap(*args, **kws):
        pdebug(f"entry: {func.__qualname__}(args: {args}, kw: {kws})")
        ret = func(*args, **kws)
        pdebug(f"exit: {func.__qualname__}(args: {args}, kw: {kws}) -> {ret}")
        return ret
    return wrap


def debug_callbacks():
    """
    (For debugging the callback module.)
    Registers a callback that matches and prints out all events.
    """
    import re
    from serve_common import callback
    @callback.register(re.compile(".*"))
    def log_callbacks(event, *data):
        pdebug(f"received callback: {event} {data}")
