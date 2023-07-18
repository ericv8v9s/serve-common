from typing import Callable, Optional, Any

from threading import Thread, Event
from socket import socket
from selectors import EVENT_READ, BaseSelector, DefaultSelector
from abc import ABC, abstractmethod
from overrides import override
import logging

from serve_common.export import *

from .selectors import SelectorRunner, do_callback_on_ready
from .sockutil import *

__all__ = [
    "AbstractSocketServer",
    "GroupServer",
    "Rendezvous"
]


logger = logging.getLogger(__name__)


@export
class AbstractSocketServer(ABC):
    """
    Abstract base class that sets up a selector and listening socket
    and accepts connections through the socket using the selector.
    """

    selector_loop: SelectorRunner
    """
    An object running select() in a loop,
    using the selector provided by the specified factory.
    Functions registered to this object as data
    will be invoked with a single file object argument.
    (See SelectorRunner and do_callback_on_ready in ipc.selectors.)
    """
    listener: socket
    """
    The socket with which this server accepts connections from.
    """
    shutdown_event: Event
    """
    Set when shutdown is called.
    """


    def __init__(self,
            selector_factory: Callable[[], BaseSelector] = DefaultSelector,
            listener_factory: Callable[[], socket] = create_listener):
        self.selector_loop = SelectorRunner(
                do_callback_on_ready, selector_factory)

        self.listener = listener_factory()
        self.selector_loop.register(self.listener, EVENT_READ, self.accept)

        self.shutdown_event = Event()


    @abstractmethod
    def accept(self, listener: socket) -> Optional[socket]:
        """
        Accepts and returns a connection (if successful).
        This function is registered to the selector.
        Subclasses may use the returned socket
        for further communication with the newly connected client.
        """
        client, _ = listener.accept()
        return client


    def start(self):
        self.selector_loop.start()


    def shutdown(self):
        """
        Tells this server to shutdown;
        sets the shutdown_event, among other clean up.
        Subclasses that override this method should call this implementation.
        """
        self.shutdown_event.set()
        self.selector_loop.stop()
        self.listener.close()


    def await_shutdown(self):
        """
        Blocks until shutdown is called and completes.
        """
        self.shutdown_event.wait()


@export
class GroupServer(AbstractSocketServer, ABC):
    """
    This server joins an IPC group generated from the server_id
    and listens to commands in the group.
    """

    def __init__(self,
            server_id: str,
            selector_factory: Callable[[], BaseSelector] = DefaultSelector,
            listener_factory: Callable[[], socket] = create_listener):
        super().__init__(selector_factory, listener_factory)
        self.server_id = server_id
        self.group = None


    @override
    def start(self):
        super().start()
        from .ipc import join_group
        self.group = join_group(self.server_id)
        self.selector_loop.register(self.group, EVENT_READ, self.on_group_msg)


    @abstractmethod
    def on_group_msg(self, group: socket) -> Optional[Any]:
        """
        Called when someone sent a message in the group.

        This default implementation receives and returns a message,
        which can be used to implement further logic in subclasses.
        """
        try:
            return recv(group)
        except EOFError:
            return None


    @override
    def shutdown(self):
        super().shutdown()
        if self.group is not None:
            self.group.close()


@export
class Rendezvous(GroupServer, ABC):
    # Meetup Protocol
    # The rendezvous is an IPC group whose name is generated from a shared id.
    #
    # The server joins this group and listens for an inquiry command.
    # Whenever it receives this command,
    # it broadcasts its address to the group.
    # The server remains in the group to greet more clients until shutdown.
    #
    # The client joins the group and sends out an inquiry.
    # If it receives a response within the allocated time window,
    # it connects to the responding server and leaves the group.
    # Otherwise, it sends another inquiry and wait again.

    GREETING = "hi"

    @override
    def on_group_msg(self, group):
        cmd = super().on_group_msg(group)
        if cmd == Rendezvous.GREETING:
            send(group, self.listener.getsockname())
        return cmd


    @staticmethod
    def request_address(server_id: str, timeout: float=None) -> bytes:
        """
        Joins the IPC group generated using the server_id
        and asks for the address of the server.
        """
        from .ipc import join_group
        from time import monotonic
        end = 0  # does nothing; this is just to make the static checker happy
        if timeout is not None:
            end = monotonic() + timeout

        retries = 3  # number of recv attempts before sending another greeting
        retries_left = 0
        delay = 10 / 1000  # 10 ms between each read

        address = None
        with join_group(server_id) as group:
            group.settimeout(delay)

            while timeout is None or monotonic() < end:
                if retries_left <= 0:
                    send(group, Rendezvous.GREETING)
                    retries_left = retries

                try:
                    # TODO handle untimely peer termination
                    # (EOFError and ConnectionResetError)
                    address = recv(group)
                    # address are bytes, commands are str
                    if isinstance(address, bytes):
                        break
                    else:
                        address = None
                except TimeoutError:
                    retries_left -= 1

        if address is None:
            raise TimeoutError

        return address


    @classmethod
    def connect_server(cls, server_id: str, timeout: float=None) -> socket:
        """
        Joins the IPC group generated using the server_id
        and attempts to establish a connection with the server.
        """
        return connect(cls.request_address(server_id, timeout))
