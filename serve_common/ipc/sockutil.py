import pickle
import struct
from socket import (
    socket,
    AF_UNIX,
    SOCK_SEQPACKET,
    SOCK_CLOEXEC,
    MSG_PEEK)

from serve_common.export import *


@export
def create_listener(addr=""):
    l = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC)
    l.bind("")  # autobind (see unix(7))
    l.listen()
    return l


@export
def connect(addr):
    s = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC)
    s.connect(addr)
    return s


# Each packet is prefixed with an 8-byte header indicating content length.
_HEADER_FMT = "=Q"  # native unsigned long long
_HEADER_LEN = struct.calcsize(_HEADER_FMT)

@export
class BadIPCMessageFormat(Exception):
    pass


@export
def send(sock, obj):
    msg = pickle.dumps(obj)
    header = struct.pack(_HEADER_FMT, len(msg))
    sock.sendall(header + msg)


@export
def recv(sock):
    header = sock.recv(_HEADER_LEN, MSG_PEEK)
    if len(header) == 0:
        raise EOFError()
    if len(header) < _HEADER_LEN:
        raise BadIPCMessageFormat()

    try:
        msglen = struct.unpack(_HEADER_FMT, header)[0]
    except struct.error:
        raise BadIPCMessageFormat()

    data = sock.recv(_HEADER_LEN + msglen)

    return pickle.loads(data[_HEADER_LEN:])
