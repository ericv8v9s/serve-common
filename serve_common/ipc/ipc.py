"""
Message based IPC using Unix domain sockets.
"""
from typing import Callable, Optional
from overrides import override

from threading import Thread, Event
import queue
import socket
import selectors
import os
import sys
from collections import defaultdict
from abc import ABC, abstractmethod

from .sockutil import *


class AbstractSocketServer(ABC):
    """
    Abstract base class that sets up a selector and listening socket
    and accepts connections through the socket using the selector.
    """

    def __init__(self,
            selector_factory: Callable[[], selectors.BaseSelector],
            listener_factory: Callable[[], socket.socket]):
        self.selector = selector_factory()
        self.listener = listener_factory()
        self.selector.register(self.listener, selectors.EVENT_READ, self.accept)

        self.shutdown_event = Event()
        def select_loop():
            while not self.shutdown_event.is_set():
                readies = self.selector.select(timeout=1)
                for key, _ in readies:
                    callback = key.data
                    try:
                        callback(key.fileobj)
                    except Exception:
                        import traceback
                        traceback.print_exc()
        self.select_loop = Thread(target=select_loop, daemon=True)

    @abstractmethod
    def accept(self, listener):
        ...

    def start(self):
        self.select_loop.start()

    def shutdown(self):
        self.shutdown_event.set()
        self.select_loop.join()
        self.selector.close()
        self.listener.close()

    def await_shutdown(self):
        self.shutdown_event.wait()


# We will assign an IPCServer instance to this to keep the obj alive.
_server = None
server_address: Optional[bytes] = None

ENVIRON_KEY = "SERVCOM_IPC_ADDR"


class MsgGroupStorage:
    """
    A two-way multimap of group IDs to connections.
    Each connection sends messages to the belonging group,
    which are then passed on to all members of that group.
    This creates a one-to-many group to connection relation,
    while the inverse is one-to-one
    (each connection can only belong in one group).

    Externally synchronized in IPCServer.
    """

    def __init__(self):
        self.conns = defaultdict(set)  # group -> connections
        self.groups = dict()  # connection -> group

    def add_conn(self, conn, group):
        self.conns[group].add(conn)
        self.groups[conn] = group

    def remove_conn(self, conn):
        group = self.groups[conn]
        self.conns[group].remove(conn)
        del self.groups[conn]


class IPCServer(AbstractSocketServer):
    def __init__(self):
        super().__init__(selectors.DefaultSelector, create_listener)
        self.mg_store = MsgGroupStorage()


    @override
    def accept(self, listener):
        # Only called when ready as determined by selector.
        try:
            conn, _ = listener.accept()
        except OSError:
            return

        try:
            # Blocking here is okay:
            # we expect new connections to immediately send the first message
            # identifying the intended group.
            group = recv(conn)
        except EOFError:
            return

        self.mg_store.add_conn(conn, group)
        self.selector.register(conn, selectors.EVENT_READ, self.new_message)


    def new_message(self, sock):
        # Only called when ready as determined by selector.
        try:
            msg = recv_raw(sock)
        except (EOFError, ConnectionError):
            sock.close()
            self.selector.unregister(sock)
            self.mg_store.remove_conn(sock)
            return

        group = self.mg_store.groups[sock]
        for c in self.mg_store.conns[group]:
            if c == sock:
                continue
            try:
                send_raw(c, msg)
            except (EOFError, ConnectionError, BlockingIOError):
                # connection closed or
                # too many messages on that connection not being read
                # TODO don't close if there is still data for us to read
                sock.close()
                self.selector.unregister(sock)
                self.mg_store.remove_conn(c)


    @override
    def await_shutdown(self):
        def await_parent_death():
            while not self.shutdown_event.wait(1):
                if os.getppid() == 1:
                    # sends shutdown command to self; recv has no timeout
                    shutdown()
        Thread(target=await_parent_death, daemon=True).start()

        # TODO correct shutdown should not depend on await_shutdown being called
        with join_group("CONTROL") as control:
            while True:
                msg = recv(control)
                if msg == "shutdown":
                    send(control, "ack_shutdown")
                    break
        self.shutdown()


def join_group(msg_group: str) -> socket.socket:
    """
    Joins a message group.

    The raw send and recv methods on returned socket should not be used,
    the send and recv functions provided by this module should be used instead.
    Messages sent through this socket will be broadcast
    to all other members in the group;
    likewise, messages from all other members can be received on this socket.

    No message is dropped implicitly,
    so the caller must be careful to receive all messages
    (and discard if uninterested) in a timely manner:
    the IPC server will close the socket if it cannot send through it.
    """
    assert server_address is not None
    s = connect(server_address)
    send(s, msg_group)
    return s


#def establish_direct(key: str) -> socket.socket:  # TODO
#    """
#    Establishes a direct connection with another process that called
#    with the same key.
#    """
#    from enum import IntEnum, auto
#
#    s = join_group(f"EST_DIRECT_{key}")
#
#    received_msgs = queue.SimpleQueue()
#    selector = selectors.DefaultSelector()
#    selector.register(s, selectors.EVENT_READ)
#    sel_loop_stop = Event()
#    def selector_loop():
#        while not sel_loop_stop.is_set():
#            selector.select(timeout=1)
#
#    class State(IntEnum):
#        LISTENING       = auto()
#        CANDIDATE_FOUND = auto()
#        DONE            = auto()
#
#    state = State.LISTENING
#    while True:
#        if state is State.LISTENING:
#            send(s, os.getpid())
#        if state is State.DONE:
#            selector.close()
#            s.close()
#            break


import base64

def b64encode(b: bytes) -> str:
    return base64.b64encode(b).decode("utf-8")
def b64decode(s: str) -> bytes:
    return base64.b64decode(s)


def setup(address=None):
    """
    Sets up the IPC module.

    If the SERVCOM_IPC_ADDR environment variable is set,
    its value is used as the address of the IPC server to connect to.
    If an address is specified by the parameter,
    the argument overrides the environment variable.
    Otherwise, a RuntimeError is raised.

    This function is idempotent.
    """
    global server_address

    try:
        server_address = b64decode(os.environ[ENVIRON_KEY])
    except KeyError:
        pass

    if address is not None:
        server_address = address

    if server_address is None or len(server_address) == 0:
        raise RuntimeError("no IPC server running")


def start() -> int:
    """
    Starts a new IPC server process and exports its address to the environment.
    Returns the PID of the server process.
    """
    from subprocess import Popen, PIPE

    entry = __name__.removesuffix(".ipc")  # use parent module as entry point
    proc = Popen([sys.executable, "-m", entry], stdout=PIPE)

    os.environ[ENVIRON_KEY] = b64encode(proc.stdout.read())
    setup()

    return proc.pid


def shutdown():
    try:
        with join_group("CONTROL") as c:
            send(c, "shutdown")
            while recv(c) != "ack_shutdown":
                pass
    except ConnectionRefusedError:
        # was not running
        pass
    except (EOFError, ConnectionResetError):
        # IPC server shutdown in the middle of our request.

        # Why do we need to deal with this race condition:
        #
        # We want SIGINT to trigger shutdown.
        #
        # SIGINT may be sent directly to us
        # or to a process group in which we belong (^C in bash).
        #
        # In the latter case,
        # the group leader is usually a server calling us as a library.
        # The server, when supported,
        # should also register a shutdown hook to shutdown us:
        # if the server was not stopped with ^C,
        # SIGINT would not be sent to us,
        # and the IPC process would run forever.
        #
        # When the shutdown hook is present
        # and the server is stopped with a ^C,
        # both the signal and the hook invoke shutdown,
        # creating this race condition.
        pass


def _spawn_proc():
    """
    Initialization for new server process.
    """
    from signal import signal, SIGINT, SIG_IGN
    from sys import stdout

    global _server, server_address
    _server = IPCServer()
    server_address = _server.listener.getsockname()

    signal(SIGINT, SIG_IGN)  # we want to be shutdown explicitly by parent

    _server.start()

    # The popen on the other end blocks until EOF or termination,
    # which is why we need to close stdout
    # (stdout.close() doesn't close the underlying fd).
    stdout.buffer.write(server_address)
    stdout.flush()
    os.close(stdout.fileno())

    _server.await_shutdown()
