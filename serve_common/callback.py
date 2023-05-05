"""
Event based callbacks.

Multiple threads/processes may `notify` events,
and each `register`ed callback will receive a copy of the event.
Registered callbacks will persist across forks,
but will not be synchronized afterwards —
child may register additional callbacks,
but they will not be observable in parent, and vice versa.

Each process interested in executing callbacks should call `callback_loop`,
and registered callbacks will execute in each of such processes.

Event data passed to `notify` must be pickleable.
If IPC is enabled, events will be sent between processes using the CALLBACK group.
"""

from functools import wraps
from threading import Thread, Lock
from queue import SimpleQueue
import re
from typing import Union

from serve_common import ipc
from serve_common.export import *


def synchronized(lock):
    def deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with lock:
                return func(*args, **kwargs)
        return wrapper
    return deco


event_queue = SimpleQueue()
callbacks = []
cb_lock = Lock()


@export
def notify(event: str, data=()):
    event_queue.put((event, data))
    if _ipc_conn is not None:
        ipc.send(_ipc_conn, (event, data))


@export
@synchronized(cb_lock)
def register(event: Union[str, re.Pattern], cb):
    callbacks.append((event, cb))


@export
def callback_loop():
    while True:
        event, data = event_queue.get()

        matched_cb = []
        with cb_lock:
            for pattern, cb in callbacks:
                if isinstance(pattern, re.Pattern):
                    if pattern.fullmatch(event):
                        matched_cb.append(cb)
                # elif str match
                elif event == pattern:
                    matched_cb.append(cb)

        for cb in matched_cb:
            try:
                cb(event, *data)
            except Exception:
                import traceback
                traceback.print_exc()
                pass

@export
def start_callback_loop():
    t = Thread(target=callback_loop, daemon=True)
    t.start()
    return t


# IPC is optional. If we don't initialize IPC,
# callbacks will function within the process.
_ipc_conn = None

@export
def enable_ipc():
    """
    Connect to IPC under the CALLBACK group.
    Also starts a thread to receive messages.
    """

    global _ipc_conn
    ipc.enable()
    _ipc_conn = ipc.join_group("CALLBACK")

    def recv_msg():
        try:
            while True:
                msg = ipc.recv(_ipc_conn)
                event_queue.put(msg)
        except EOFError:
            pass
    Thread(target=recv_msg, daemon=True).start()


@export
def initialize(ipc=False):
    Thread(target=callback_loop, daemon=True).start()
    if ipc:
        enable_ipc()


@export
def post_fork():
    """
    Call in child process after fork to restart threads.
    """
    # If _ipc_conn is not None, it was initialized,
    # but should not be used in child.
    if _ipc_conn is not None:
        _ipc_conn.close()
        initialize(ipc=True)
    else:
        initialize()
