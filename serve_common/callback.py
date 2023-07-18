"""
Event based callbacks.

Multiple threads/processes may ``notify`` events,
and each ``register``ed callback will receive a copy of the event.
Registered callbacks will persist across forks,
but will not be synchronized afterwards —
child may register additional callbacks,
but they will not be observable in parent, and vice versa.

Event data passed to ``notify`` must be pickleable.
If IPC is enabled, events will be sent between processes using the callback group.
"""

import threading
from threading import Thread, RLock, Event
from queue import SimpleQueue
import re
import os
from typing import Union

from serve_common import ipc
from serve_common.threads import synchronized
from serve_common.export import *

__all__ = [
    "get_completion_name",
    "Callback",
    "register",
    "notify",
    "wait_for",
    "notify_and_wait"
]


CALLBACK_GROUP = __name__

event_queue = SimpleQueue()
callbacks = {}
cb_counter = 0
cb_lock = RLock()

@synchronized(cb_lock)
def _gen_next_cbid():
    global cb_counter
    nextid = cb_counter
    cb_counter += 1
    return (os.getpid(), nextid)

@synchronized(cb_lock)
def _add_callback(cbobj):
    callbacks[cbobj.cbid] = cbobj

@synchronized(cb_lock)
def _remove_callback(cbid):
    del callbacks[cbid]


@export
def get_completion_name(event: str):
    return "completed:" + event

@export
class Callback:
    def __init__(self, event, cb):
        self.event = event
        self.cb = cb
        self.cbid = _gen_next_cbid()

        self.is_single_use = False
        self.event_param_ignored = False
        self.cmpl_ack_enabled = False

    def single_use(self):
        if self.is_single_use:
            return self
        cb = self.cb
        def wrapper(event, *data):
            cb(event, *data)
            _remove_callback(self.cbid)
        self.cb = wrapper
        self.is_single_use = True
        return self

    def ignore_event(self):
        if self.event_param_ignored:
            return self
        cb = self.cb
        self.cb = lambda event, *data: cb(*data)
        self.event_param_ignored = True
        return self

    def send_completion_ack(self, ack_event=None):
        if self.cmpl_ack_enabled:
            return self
        if ack_event is None:
            ack_event_gen = get_completion_name
        else:
            ack_event_gen = lambda e: ack_event
        cb = self.cb
        def wrapper(event, *data):
            cb(event, *data)
            notify(ack_event_gen(event))
        self.cb = wrapper
        self.cmpl_ack_enabled = True
        return self

    def register(self):
        _add_callback(self)


@export
def register(event: Union[str, re.Pattern],
        cb=None,
        *,
        ack_completion: Union[bool, str]=False,
        pass_event: bool=True):
    """
    Registers the callback to be invoked when event is receieved.
    If pass_event is False,
    the event argument will not be passed to the callback.
    If a string is given to ack_completion,
    an event with that name will be notified when this callback completes;
    if True is given, the acknowledgement event name will be generated
    by prefixing 'completed:' to the name of the event triggering this callback.
    If cb is None, the return value is a decorator
    that will register the specified function.
    """

    original_function = cb

    if cb is None:
        # can't use partial: breaks with stacked register
        return (lambda cb: register(event, cb=cb,
                ack_completion=ack_completion,
                pass_event=pass_event))

    assert callable(cb)

    cb = Callback(event, cb)

    if not pass_event:
        cb.ignore_event()

    # A boolean true => generated event name
    # A string       => used verbatim
    # A falsy value  => do not send comfirmation
    # Otherwise      => ValueError
    # Note that non-empty strings are also truthy,
    # necessitating the manual bool check.
    if isinstance(ack_completion, bool) and ack_completion:
        cb.send_completion_ack()
    elif isinstance(ack_completion, str):
        cb.send_completion_ack(ack_completion)
    elif not ack_completion:
        pass
    else:
        raise ValueError()

    cb.register()
    return original_function


@export
def notify(event: str, data=()):
    if _callback_loop_thread is None or not _callback_loop_thread.is_alive():
        raise RuntimeError("callback module not initialized")

    if not isinstance(data, tuple):
        raise TypeError()
    event_queue.put((event, data))
    if _ipc_conn is not None:
        ipc.send(_ipc_conn, (event, data))


class WaitContext:
    def __init__(self, event: str):
        self.event = Event()
        (Callback(event, lambda _: self.event.set())
            .single_use().register())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if None is exc_type is exc_val is exc_tb:
            self.event.wait()
        return False

    def wait(self, timeout=None) -> bool:
        return self.event.wait(timeout=timeout)

@export
def wait_for(event: str) -> WaitContext:
    """
    Returns a context manager that blocks on exit until the event is observed
    (except if an exception occurred).

    Note that calling this function after starting the operation
    that would trigger the target event creates a race condition:
    when the operation completes and sends the completion event,
    the caller may have not been setup in time to receive the completion event,
    causing the event to be missed and any subsequent wait() to wait forever.
    """
    return WaitContext(event)


@export
def notify_and_wait(event: str, data=(), ack_event=None, timeout=None):
    """
    Notifies and waits for a completion acknowledgement.
    The completion acknowledgement would only be sent back if the event
    was registered to produce one.
    """
    if ack_event is None:
        ack_event = get_completion_name(event)
    completion_event = wait_for(ack_event)
    notify(event, data)
    completion_event.wait(timeout)


_callback_loop_thread: Thread = None

def _callback_loop():
    while True:
        event, data = event_queue.get()

        matched_cb = []
        with cb_lock:
            for cb in callbacks.values():
                if isinstance(cb.event, re.Pattern):
                    if cb.event.fullmatch(event):
                        matched_cb.append(cb)
                # elif str match
                elif event == cb.event:
                    matched_cb.append(cb)

        for cb in matched_cb:
            try:
                cb.cb(event, *data)
            except Exception:
                import traceback
                traceback.print_exc()
                pass


# IPC is optional. If we don't initialize IPC,
# callbacks will function within the process.
_ipc_conn = None

def enable_ipc():
    """
    Connect to IPC under the callback group.
    Also starts a thread to receive messages.
    """
    global _ipc_conn
    if _ipc_conn is not None:
        return

    ipc.setup()
    _ipc_conn = ipc.join_group(CALLBACK_GROUP)

    def recv_msg():
        try:
            while True:
                msg = ipc.recv(_ipc_conn)
                event_queue.put(msg)
        except EOFError:
            pass
    Thread(target=recv_msg, daemon=True).start()


def initialize(ipc=False):
    global _callback_loop_thread
    if _callback_loop_thread is None or not _callback_loop_thread.is_alive():
        _callback_loop_thread = Thread(target=_callback_loop, daemon=True)
        _callback_loop_thread.start()

    if ipc:
        enable_ipc()


def post_fork():
    """
    Call in child process after fork to restart threads.
    """
    global _ipc_conn
    # If _ipc_conn is not None, it was initialized,
    # but should not be used in child.
    if _ipc_conn is not None:
        _ipc_conn.close()
        _ipc_conn = None  # force reinit _ipc_conn
        initialize(ipc=True)
    else:
        initialize()
