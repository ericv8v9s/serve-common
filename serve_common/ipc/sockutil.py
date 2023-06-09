from typing import Any

import os
import pickle
import struct
import socket
from socket import (
    AF_UNIX,
    SOCK_SEQPACKET,
    SOCK_CLOEXEC,
    MSG_PEEK)

from serve_common.export import *


FAMILY = AF_UNIX
TYPE = SOCK_SEQPACKET | SOCK_CLOEXEC


@export
def socketpair(family=FAMILY, type=TYPE, proto=0):
    a, b = socket.socketpair(family, type, proto)
    os.set_inheritable(a.fileno(), True)
    os.set_inheritable(b.fileno(), True)
    return a, b


@export
def create_listener(addr=""):
    l = socket.socket(FAMILY, TYPE)
    l.bind(addr)  # autobind (see unix(7))
    l.listen()
    return l


@export
def connect(addr):
    s = socket.socket(FAMILY, TYPE)
    s.connect(addr)
    return s


# Each packet is prefixed with an 8-byte header indicating content length.
_HEADER_FMT = "=Q"  # native unsigned long long
_HEADER_LEN = struct.calcsize(_HEADER_FMT)

@export
class BadIPCMessageFormat(Exception):
    pass


@export
def send_raw(sock, buf: bytes):
    header = struct.pack(_HEADER_FMT, len(buf))
    sock.sendall(header + buf)

@export
def send(sock, obj: Any):
    send_raw(sock, pickle.dumps(obj))


@export
def recv_raw(sock) -> bytes:
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

    return data[_HEADER_LEN:]

@export
def recv(sock) -> Any:
    return pickle.loads(recv_raw(sock))
