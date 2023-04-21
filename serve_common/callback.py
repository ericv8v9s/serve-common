"""
Multi-producer, multi-consumer, event based callbacks.

Multiple threads/processes may `notify` events,
and each `register`ed callback will receive a copy of the event.
Registered callbacks will persist across forks,
but will not be synchronized afterwards —
child may register additional callbacks,
but they will not be observable in parent, and vice versa.

Each process interested in executing callbacks should call `callback_loop`
(possibly in a thread),
and registered callbacks will execute in each of such processes.

Event data passed to `notify` must be pickleable.
"""

import os
import threading
import multiprocessing as mp
from functools import wraps


def synchronized(lock):
    def deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with lock:
                return func(*args, **kwargs)
        return wrapper
    return deco


# shared between processes, managed by multiprocessing
_manager = None
_raw_events = mp.SimpleQueue()
_event_queues_lock = mp.Lock()
# process local proxy object, effectively shared through manager
_event_queues = None

@synchronized(_event_queues_lock)
def init_shared_storage(manager=None):
    global _manager, _event_queues
    if manager is None:
        manager = mp.Manager()

    _manager = manager
    _event_queues = _manager.list()

    def distribute_events():
        while True:
            event = _raw_events.get()
            with _event_queues_lock:
                for queue in _event_queues:
                    queue.put(event)

    mp.Process(
        target=distribute_events,
        daemon=True
    ).start()


def notify(event: str, data=()):
    _raw_events.put((event, data))


# process local, each process will see its own copy of these variables
_cb_lock = threading.Lock()
_callbacks = {}

_current_pid = -1


@synchronized(_cb_lock)
def register(event: str, cb):
    if event not in _callbacks:
        _callbacks[event] = []
    _callbacks[event].append(cb)

@synchronized(_cb_lock)
def registered_events():
    return list(_callbacks.keys())


def start_callback_loop():
    if _current_pid == os.getpid():
        return
    _CallbackLoop().start()


class _CallbackLoop(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.events = _manager.Queue()
        with _event_queues_lock:
            _event_queues.append(self.events)

    def run(self) -> None:
        while True:
            event, args = self.events.get()
            if event in _callbacks:
                cbs = _callbacks[event]
                for cb in cbs:
                    try:
                        cb(event, *args)
                    except Exception:
                        pass
