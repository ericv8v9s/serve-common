from typing import Callable, Any
from threading import RLock, Event, current_thread
from selectors import *
import logging
import os
from enum import IntEnum, auto
import queue

from serve_common.export import *
from serve_common.threads import *
from serve_common.states import *

logger = logging.getLogger(__name__)

__all__ = [
    "SelectorRunner",
    "do_callback_on_ready"
]


@export
class SelectorRunner:
    def __init__(self,
            on_ready: Callable[[SelectorKey, int], Any],
            selector_factory: Callable[[], BaseSelector] = DefaultSelector):
        self.impl = _SelectorRunnerImpl(on_ready, selector_factory)

    def register(self, fileobj, events, data=None):
        return self.impl.modify_selector("register", fileobj, events, data)

    def unregister(self, fileobj):
        return self.impl.modify_selector("unregister", fileobj)

    def modify(self, fileobj, events, data=None):
        return self.impl.modify_selector("modify", fileobj, events, data)

    def start(self):
        self.impl.start()

    def stop(self):
        self.impl.stop()


class _SelectorRunnerImpl(WaitMixin, MessageMixin, StateMachine):
    class Command(IntEnum):
        MODIFY    = auto()
        STOP      = auto()


    def __init__(self,
            on_ready: Callable[[SelectorKey, int], Any],
            selector_factory: Callable[[], BaseSelector] = DefaultSelector):
        super().__init__(self.INIT)

        # Linearizes external access.
        self.lock = RLock()

        # Theoretically, only epoll(7) (and thus EpollSelector)
        # allow register() in one thread while another is waiting in select()
        # (poll(2) and select(2) are effectively single threaded).
        #
        # We use a state machine to control access to the selector.
        # Unfortunately, select() blocks indefinitely without timeout,
        # which means we face arbitrarily long delays
        # when requesting a modification while select() is in progress.
        # The solution here creates a pipe and register the read end
        # with the selector to notify and interrupt its select().

        # The state machine is not guarded by anything:
        # state modification is limited to the runner thread.
        @thread_target
        def runner():
            while not self.is_terminated():
                prev = self.step()
                # pdebug(f"selector: {prev.__name__} -> {self.state.__name__}")
        self.runner = runner

        self.selector = selector_factory()
        self.on_ready = on_ready

        self.ctrl_read, self.ctrl_write = os.pipe()
        self.modify_complete = Event()
        self.selector.register(self.ctrl_read, EVENT_READ)


    def _interrupt_select(self):
        os.write(self.ctrl_write, b'\0')


    def INIT(self):
        # Initial state; runner not started.
        return self.DISPATCH


    def DISPATCH(self):
        # If we have a command, process it; otherwise, wait for select.
        try:
            match self.messages.get_nowait():
                case _SelectorRunnerImpl.Command.MODIFY:
                    return self.MODIFYING
                case _SelectorRunnerImpl.Command.STOP:
                    return self.STOPPING
                case _ as cmd:
                    raise RuntimeError(f"invalid command: {cmd}")
        except queue.Empty:
            return self.SELECTING


    def SELECTING(self):
        # Waiting for select.
        readies = self.selector.select()
        for key, events in readies:
            if key.fd == self.ctrl_read:
                if len(os.read(self.ctrl_read, 1)) == 0:
                    raise RuntimeError("selector control pipe closed")
            else:
                try:
                    self.on_ready(key, events)
                except Exception:
                    logger.exception("Exception caught in selector loop:")
        return self.DISPATCH


    def MODIFYING(self):
        # Wait for external thread to modify selector.
        # Pauses the loop and yields control.
        self.modify_complete.wait()
        return self.DISPATCH


    def STOPPING(self):
        self.selector.unregister(self.ctrl_read)
        os.close(self.ctrl_write)
        os.close(self.ctrl_read)
        self.selector.close()
        return self.TERMINATED


    @sync_method("lock")
    def modify_selector(self, method, *args, **kws):
        if self.state == self.INIT:
            # The correctness of this check depends on the assumption that
            # self.state will not change once the check succeeds.
            # This can be guaranteed because if state is in INIT,
            # the runner thread
            # (which is the only entity allowed to modify the state)
            # has not started and will not be started.
            return getattr(self.selector, method)(*args, **kws)
        if current_thread() == self.runner:
            # The point of this method is to prevent modification
            # while select is happening.
            # The runner thread does the select;
            # if its here, it can't be selecting.
            return getattr(self.selector, method)(*args, **kws)

        self.modify_complete.clear()
        self.messages.put(_SelectorRunnerImpl.Command.MODIFY)
        self._interrupt_select()
        # Will go to MODIFYING on next DISPATCH.
        self.wait_for(self.MODIFYING)
        result = getattr(self.selector, method)(*args, **kws)
        self.modify_complete.set()
        return result


    @sync_method("lock")
    def start(self):
        self.runner.start()


    @sync_method("lock")
    def stop(self):
        self.messages.put(_SelectorRunnerImpl.Command.STOP)
        self._interrupt_select()
        self.wait_for(self.TERMINATED)


@export
def do_callback_on_ready(key, events):
    """
    Assuming that the file object was registered
    with a callback as its opaque data,
    retrieves the callback from the passed in selector key
    and invokes the callback.
    The callback will be invoked with the file object as the sole argument,
    events are ignored.

    This function is for convenience to be passed to selector_loop and friends.
    """
    callback = key.data
    callback(key.fileobj)
