from typing import Union, Callable, Any
from types import ModuleType

import sys
import os
from subprocess import Popen, PIPE
import pickle
import threading
from functools import partial
from inspect import signature, Parameter

from serve_common.threads import *
from serve_common.export import *

__all__ = [
    "Chain",
    "set_process_name",
    "spawn_process",
    "event_shutdown",
    "SHUTDOWN_SIGNALS",
    "exit_on_signals",
    "exit_on_parent_death"
]

import logging
logger = logging.getLogger(__name__)


@export
class Chain:
    def _require_no_param(self, func: Callable[[], Any]):
        sig = signature(func)
        missing_params = []
        # A missing param is one that:
        # - was not provided (thus not partial'ed in)
        # - is not optional, by:
        #   - not having a default value
        #   - not var positional (i.e. *args)
        #   - not var keyword (i.e. **kwargs)
        for param in sig.parameters.values():
            if (param.default == Parameter.empty
                    and param.kind != Parameter.VAR_POSITIONAL
                    and param.kind != Parameter.VAR_KEYWORD):
                missing_params.append(param)
        if len(missing_params) > 0:
            param_str = ", ".join(map(str, missing_params))
            raise TypeError(f"missing arguments {func}({param_str})")


    def __init__(self, *targets: Callable[[], Any]):
        for t in targets:
            self._require_no_param(t)
        self.targets = list(targets)


    def then(self, target: Callable, *args, **kwargs):
        if not (len(args) == 0 and len(kwargs) == 0):
            target = partial(target, *args, **kwargs)
        self._require_no_param(target)
        self.targets.append(target)
        return self


    def __call__(self):
        for target in self.targets:
            target()


@export
def set_process_name(name: str):
    """Sets the current process's name."""
    import multiprocessing as mp
    mp.current_process().name = name


@export
def spawn_process(target: Union[str, ModuleType, Callable], *args) -> int:
    """
    Starts a new process that runs the target;
    returns the new process's PID.
    The target may be a module or a full path to a module
    to use as the entrypoint;
    or a pickable-callable that will be sent to and run in the new process.
    """
    if isinstance(target, ModuleType):
        target = target.__name__

    if isinstance(target, str):
        proc = Popen([sys.executable, "-m", target, *args], stderr=sys.stderr)
    elif callable(target):
        proc = Popen([sys.executable, "-m", "serve_common.compute"],
                stdin=PIPE, stderr=sys.stderr)
        proc.stdin.write(pickle.dumps([target, *args]))
        proc.stdin.close()
    else:
        raise TypeError()

    return proc.pid


from signal import SIGINT, SIGHUP, SIGTERM
SHUTDOWN_SIGNALS = (SIGINT, SIGHUP, SIGTERM)
"""Signals that commonly mean (soft) termination."""
export("SHUTDOWN_SIGNALS")

event_shutdown = threading.Event()
"""
Set when shutdown initiated.
This event can be waited for or set in the child process.
"""
export("event_shutdown")


@export
def exit_on_signals(*signals):
    """
    Sets event_shutdown when any of the listed signals are caught.
    """
    from signal import signal
    for signum in signals:
        signal(signum, lambda *_: event_shutdown.set())

@export
def exit_on_parent_death():
    """
    Sets event_shutdown when the parent process dies.
    """
    @thread_target(daemon=True)
    def wait_parent_death():
        while True:
            if event_shutdown.wait(1):
                break
            if os.getppid() == 1:  # adopted by init
                event_shutdown.set()
    wait_parent_death.start()
