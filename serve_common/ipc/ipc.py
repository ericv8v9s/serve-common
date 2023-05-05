"""
Message based IPC using Unix domain sockets.
"""


from threading import Thread, Event
import queue
import socket
import selectors
import os

from collections import defaultdict

from .sockutil import *


# We will assign an IPCServer instance to this to keep the obj alive.
_server = None
server_address: bytes = None


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
        self.groups = defaultdict(set)
        self.conns = dict()

    def add_conn(self, conn, group):
        self.groups[group].add(conn)
        self.conns[conn] = group

    def remove_conn(self, conn):
        group = self.conns[conn]
        self.groups[group].remove(conn)
        del self.conns[conn]


class IPCServer:
    def __init__(self):
        self.selector = selectors.DefaultSelector()

        self.listener = create_listener()
        #self.listener.setblocking(False)
        self.selector.register(self.listener, selectors.EVENT_READ, self.accept)

        self.mg_store = MsgGroupStorage()

        self.select_loop = Thread(target=self.run)
        self.shutdown_event = Event()


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

        #conn.setblocking(False)
        self.selector.register(conn, selectors.EVENT_READ, self.new_message)


    def new_message(self, sock):
        # Only called when ready as determined by selector.
        try:
            msg = recv(sock)
        except (EOFError, ConnectionError):
            sock.close()
            self.selector.unregister(sock)
            self.mg_store.remove_conn(sock)
            return

        group = self.mg_store.conns[sock]
        for c in self.mg_store.groups[group]:
            if c == sock:
                continue
            try:
                send(c, msg)
            # except BlockingIOError:
            #     pass  # too many messages on that connection not being read
            except EOFError:
                sock.close()
                self.selector.unregister(sock)
                self.mg_store.remove_conn(c)


    def run(self):
        while not self.shutdown_event.is_set():
            readies = self.selector.select(timeout=1)
            for key, events in readies:
                callback = key.data
                try:
                    callback(key.fileobj)
                except Exception as e:
                    import traceback
                    traceback.print_exc()
        self.selector.close()


    def start(self):
        self.select_loop.start()
        return self.select_loop


    def await_shutdown(self):
        with join_group("CONTROL") as ctrl:
            while True:
                msg = recv(ctrl)
                if msg == "shutdown":
                    send(ctrl, "ack_shutdown")
                    break

        self.shutdown_event.set()
        self.select_loop.join()
        self.listener.close()


def join_group(msg_group: str) -> socket.socket:
    assert server_address is not None
    s = connect(server_address)
    send(s, msg_group)
    return s


def enable():
    if server_address is None:
        start()

def start() -> int:
    """Starts a new IPC server process and returns its PID."""
    import sys
    from subprocess import Popen, PIPE

    entry = __name__.removesuffix(".ipc")  # use parent module as entry point
    proc = Popen([sys.executable, "-m", entry], stdout=PIPE)

    global server_address
    server_address = proc.stdout.read()
    return proc.pid


def shutdown():
    try:
        with join_group("CONTROL") as c:
            send(c, "shutdown")
            recv(c)  # waits for ack
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
    from signal import signal, SIGINT
    from sys import stdout

    global _server, server_address
    _server = IPCServer()
    server_address = _server.listener.getsockname()

    signal(SIGINT, lambda signum, frame: shutdown())

    _server.start()

    # The popen on the other end blocks until EOF or termination,
    # which is why we need to close stdout
    # (stdout.close() doesn't close the underlying fd).
    stdout.buffer.write(server_address)
    stdout.flush()
    os.close(stdout.fileno())

    _server.await_shutdown()
