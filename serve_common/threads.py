"""
Multi-threading related utilities.
"""
from threading import Thread, RLock
from functools import partial, wraps
from time import monotonic

from overrides import override

from serve_common.export import *

__all__ = [
    "thread_target",
    "synchronized",
    "sync_method"
]


@export
def thread_target(target=None, /, **kws):
    """
    Turns the decorated callable into a ``Thread``
    that calls the callable as its target.
    Arguments to this function are passed to the ``Thread`` constructor.
    """
    if target is None:
        return partial(thread_target, **kws)
    return Thread(target=target, **kws)


@export
def synchronized(lock):
    """
    Guards the decorated function with the provided lock.
    """
    def deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with lock:
                return func(*args, **kwargs)
        return wrapper
    return deco


@export
def sync_method(lock_attr):
    """
    Synchronizes the method using the lock named by the parameter.
    The attribute of the name will be looked up at runtime and used as the lock.
    """
    def deco(func):
        @wraps(func)
        def wrap(self, *args, **kws):
            with getattr(self, lock_attr):
                return func(self, *args, **kws)
        return wrap
    return deco
