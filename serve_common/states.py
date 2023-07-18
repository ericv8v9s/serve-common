from threading import RLock, Condition, Semaphore
from queue import SimpleQueue
from time import monotonic

from serve_common.utils import repr_call
from serve_common.export import *

__all__ = [
    "StateMachine",
    "WaitMixin",
    "MessageMixin"
]


@export
class StateMachine:
    """
    Base class for defining state machines.

    States can be defined by inheriting this class and adding methods.
    Each such methods may choose to take arguments,
    which must be provided by the previous state,
    and return one of the following values:
        - the next state to step to as another Callable; or
        - the next state with arguments, packed as (state, (args...)).
    """

    def __init__(self, initial_state):
        super().__init__()
        self.state = initial_state
        self.__args = ()

    def is_terminated(self):
        return self.state == self.TERMINATED

    def TERMINATED(self):
        """The final terminated state."""
        return self.TERMINATED

    def step(self):
        """Steps to the specified state and returns the previous state."""

        if self.is_terminated():
            raise RuntimeError("state machine already terminated")

        previous = self.state
        self.state = self.state(*self.__args)

        match self.state:
            case (state, args):
                self.state = state
                self.__args = args
            case state if callable(state):
                self.state = state
                self.__args = ()
            case invalid_state:
                raise TypeError(
                        "cannot interpret next state:"
                       f" {repr_call(previous, *self.__args)} ->"
                       f" {repr(invalid_state)}")
        return previous


@export
class WaitMixin:
    """Adds ``wait_for``, which allows waiting for a specific state."""

    def __init__(self, *args, **kws):
        super().__init__(*args, **kws)
        self.__enter_wait = RLock()
        self.__num_waiting = 0
        # Number of threads waiting.
        self.__cond = Condition()
        # For notifying waiting threads.
        self.__sema = Semaphore(0)
        # Number of checks completed,
        # used to make sure every waiting thread has seen the state update.


    def wait_for(self, state, timeout=None):
        with self.__enter_wait:
            # Block threads from starting their wait
            # while a notify is taking place.
            pass

        with self.__cond:
            self.__num_waiting += 1

            if timeout is None:
                while True:
                    matched = self.state == state
                    self.__sema.release()
                    # "I got the notify and have checked the current state."
                    if matched:
                        break
                    else:
                        self.__cond.wait()
            else:
                while True:
                    start_time = monotonic()
                    matched = self.state == state
                    self.__sema.release()
                    if matched:
                        break
                    else:
                        timeout -= monotonic() - start_time
                        if timeout <= 0 or not self.__cond.wait(timeout):
                            break  # timed out
            self.__num_waiting -= 1
            return matched


    def step(self):
        ret = super().step()

        with self.__enter_wait:
            # Freeze current state of waiting threads;
            # do not allow more threads to start waiting.
            with self.__cond:
                self.__cond.notify_all()
                num_waiting = self.__num_waiting
            for _ in range(num_waiting):
                self.__sema.acquire()
                # Make sure all waiting threads see the update.
        return ret


@export
class MessageMixin:
    """Adds a queue ``messages``."""

    def __init__(self, *args, **kws):
        super().__init__(*args, **kws)
        self.messages = SimpleQueue()
