"""
Message based IPC using Unix domain sockets.
"""

# TODO redesign to use ZeroMQ

from typing import Optional
from overrides import override

import socket
from selectors import EVENT_READ
import os
import sys
from collections import defaultdict
import logging

from serve_common.export import *
from serve_common.threads import thread_target

from .server import AbstractSocketServer
from .sockutil import *

logger = logging.getLogger(__name__)
# TODO sort out LogServer startup
# IPC has to start first,
# otherwise the log server would never get the IPC server address.
# This means the IPC process, during startup,
# cannot produce logs to the log server until it, too, has started.
# Proposals:
# a) queue logs until a server is connected, then flush the queue
#    undesirable: a thread need to be started in each handler and poll
# b) rewrite logging server to make it independent of IPC system
#    undesirable: this makes effectively a second IPC system
# c) make IPC system's logging separate (it's own config to it's own file)
# d) start IPC in a "bootstrap" mode, logging with user provided config;
#    when a log server is started, switch over to that.
#    - when log server is ready, it needs to somehow signal it
#      (serve_common.callback?)
#    - IPC module needs to provide an API to change it's logging setup
# e) disable logging in IPC


__all__ = [
    "server_address",
    "join_group",
    "setup",
    "start",
    "shutdown"
]


class ServerNotInitializedError(Exception):
    """
    Thrown when an operation that requires a running IPC server is attempted,
    but no server is running.
    """
    pass


# We will assign an IPCServer instance to this to keep the obj alive.
_server = None
server_address: Optional[bytes] = None
export("server_address")

ENVIRON_KEY = "SERVCOM_IPC_ADDR"

_CONTROL_GROUP = f"{__name__}:control"


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
        if conn not in self.groups.keys():
            return
        group = self.groups[conn]
        self.conns[group].remove(conn)
        del self.groups[conn]


class IPCServer(AbstractSocketServer):
    def __init__(self):
        super().__init__()
        self.mg_store = MsgGroupStorage()

        @thread_target(daemon=True)
        def shutdown_listener():
            with join_group(_CONTROL_GROUP) as control:
                while not self.shutdown_event.is_set():
                    cmd = recv(control)
                    if cmd == "shutdown":
                        self.shutdown()
                        break
        self.shutdown_listener = shutdown_listener


    @override
    def start(self):
        super().start()
        self.shutdown_listener.start()


    @override
    def accept(self, listener):
        client = super().accept(listener)

        # Blocking here is okay:
        # we expect new connections to immediately send the first message
        # identifying the intended group.
        group = recv(client)

        client.setblocking(False)

        self.mg_store.add_conn(client, group)
        self.selector_loop.register(client, EVENT_READ, self.new_message)


    def new_message(self, sock):
        try:
            msg = recv_raw(sock)
        except (EOFError, ConnectionError):
            self.selector_loop.unregister(sock)
            sock.close()
            self.mg_store.remove_conn(sock)
            return

        group = self.mg_store.groups[sock]
        members = list(self.mg_store.conns[group])
        # Copy to avoid concurrent modification.
        for c in members:
            if c == sock:
                continue
            try:
                send_raw(c, msg)
            except (OSError, ConnectionError, BlockingIOError):
                # connection failed or
                # too many messages on that connection not being read
                # TODO don't close if there is still data for us to read
                self.selector_loop.unregister(sock)
                sock.close()
                self.mg_store.remove_conn(c)


@export
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
    if server_address is None:
        raise ServerNotInitializedError
    s = connect(server_address)
    send(s, msg_group)
    return s


import base64

def b64encode(b: bytes) -> str:
    return base64.b64encode(b).decode("utf-8")
def b64decode(s: str) -> bytes:
    return base64.b64decode(s)


@export
def setup(address=None):
    """
    Sets up the IPC module.

    If the SERVCOM_IPC_ADDR environment variable is set,
    its value is used as the address of the IPC server to connect to.
    If an address is specified by the parameter,
    the argument overrides the environment variable.
    Otherwise, ServerNotInitializedError is raised.

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
        raise ServerNotInitializedError


@export
def start() -> int:
    """
    Starts a new IPC server process and exports its address to the environment.
    Returns the PID of the server process.
    """
    logger.info("starting IPC")

    from subprocess import Popen, PIPE
    from sys import stderr

    entry = __name__.removesuffix(".ipc")  # use parent module as entry point
    proc = Popen([sys.executable, "-m", entry], stdout=PIPE, stderr=stderr)

    os.environ[ENVIRON_KEY] = b64encode(proc.stdout.read())
    setup()

    logger.info("IPC ready")
    return proc.pid


@export
def shutdown():
    try:
        with join_group(_CONTROL_GROUP) as c:
            send(c, "shutdown")
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

    global _server, server_address
    _server = IPCServer()
    server_address = _server.listener.getsockname()

    # we want to be shutdown explicitly by parent or parent death
    signal(SIGINT, SIG_IGN)

    @thread_target
    def await_parent_death():
        import os
        while not _server.shutdown_event.wait(1):
            if os.getppid() == 1:
                _server.shutdown()
                break
    await_parent_death.start()

    logger.debug("staring IPC server")
    _server.start()
    logger.debug("IPC server started")

    from sys import stdout
    # The popen on the other end blocks until EOF or termination,
    # which is why we need to close stdout
    # (stdout.close() doesn't close the underlying fd).
    stdout.buffer.write(server_address)
    stdout.flush()
    os.close(stdout.fileno())

    _server.await_shutdown()
