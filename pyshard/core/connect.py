import abc
import struct
import logging
import socket
import asyncio

from .typing import Codec

logger = logging.getLogger(__name__)
Kb = 1024


def mksock(host, port, backlog=5, mode='l') -> socket.socket:
    """
    Returns non-blocking socket

    :param host:
    :param port:
    :param backlog:
    :param mode:
    :return:
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setblocking(False)
    if mode == 'l':
        sock.bind((host, port))
        sock.listen(backlog)

    return sock


class ProtocolABC(abc.ABC):
    @abc.abstractmethod
    def _pack(self, obj: bytes) -> bytes: ...
    @abc.abstractmethod
    def do_send(self, bytes_data: bytes, sock: socket.socket) -> None: ...
    @abc.abstractmethod
    def do_recv(self, sock: socket.socket) -> bytes: ...


class AsyncProtocolABC(abc.ABC):
    @abc.abstractmethod
    def _pack(self, obj: bytes) -> bytes: ...
    @abc.abstractmethod
    async def do_send(self, bytes_data: bytes, conn: socket.socket) -> None: ...
    @abc.abstractmethod
    async def do_recv(self, sock: socket.socket) -> bytes: ...


class ConnectionABC(abc.ABC):
    @abc.abstractmethod
    def connect(self) -> None: ...
    @abc.abstractmethod
    def send(self, str_obj: str) -> None: ...
    @abc.abstractmethod
    def recv(self) -> str: ...


class Protocol(ProtocolABC):
    def __init__(self, buffer_size: int=Kb, codec: Codec='utf-8'):
        self._prefix = struct.Struct('I')
        self._buffer_size = buffer_size
        self._codec = codec

    def _pack(self, obj):
        prefix = self._prefix.pack(len(obj))
        return prefix + obj

    def do_send(self, bytes_data: bytes, sock=None):
        sock = sock or self._sock
        sock.sendall(self._pack(bytes_data))

    def do_recv(self, sock=None):
        data = b''
        total = 0
        sock = sock or self._sock

        prefix = sock.recv(self._prefix.size)
        logger.debug(f"Peer received prefix: {prefix}")
        if not prefix:
            raise RuntimeError('Connection was closed by peer')

        msg_len = self._prefix.unpack(prefix)[0]
        logger.debug(f"Peer will receive message of length {msg_len} bytes")

        while total < msg_len:
            buff_size = min(msg_len - total, self._buffer_size)
            chunk = sock.recv(buff_size)
            if not chunk:
                raise AssertionError(f'Expected {msg_len} bytes, received: {len(data)} bytes')

            data += chunk
            total += buff_size

        return data


class AsyncProtocol(AsyncProtocolABC):
    def __init__(self, buffer_size: int=Kb, loop=None, codec: Codec='utf-8'):
        self._prefix = struct.Struct('I')
        self._buffer_size = buffer_size
        self._codec = codec
        self._loop = loop if loop else asyncio.get_event_loop()

    def _pack(self, obj):
        prefix = self._prefix.pack(len(obj))
        return prefix + obj

    async def do_send(self, bytes_data: bytes, conn):
        await self._loop.sock_sendall(conn, self._pack(bytes_data))

    async def do_recv(self, conn):
        data = b''
        total = 0

        prefix = await self._loop.sock_recv(conn, self._prefix.size)
        logger.debug(f"Peer received prefix: {prefix}")
        if not prefix:
            raise RuntimeError('Connection was closed by peer')
            
        msg_len = self._prefix.unpack(prefix)[0]
        logger.debug(f"Peer will receive message of length {msg_len} bytes")

        while total < msg_len:
            buff_size = min(msg_len - total, self._buffer_size)
            chunk = await self._loop.sock_recv(conn, buff_size)
            if not chunk:
                raise AssertionError(f'Expected {msg_len} bytes, received: {len(data)} bytes')

            data += chunk
            total += buff_size

        return data


def _to_bytes(str_obj: str, codec: Codec) -> bytes:
        return bytes(str_obj, encoding=codec)


def _from_bytes(bytes_obj: bytes, codec: Codec) -> str:
        return bytes_obj.decode(codec)


class ConnectionBase(ConnectionABC, Protocol):
    def __init__(self, host: str=None, port: int=None, **protocol_kwargs):
        if host and port:
            self._addr = (host, port)

        super(ConnectionBase, self).__init__(**protocol_kwargs)

    def connect(self):
        raise NotImplementedError()

    def send(self, str_obj):
        bytes_obj = self._to_bytes(str_obj)

        self.do_send(bytes_obj)

    def _to_bytes(self, str_obj: str) -> bytes:
        return bytes(str_obj, encoding=self._codec)

    def recv(self):
        bytes_obj = self.do_recv()

        return self._from_bytes(bytes_obj)

    def _from_bytes(self, bytes_obj: bytes) -> str:
        return bytes_obj.decode(self._codec)

    def close(self):
        raise NotImplementedError()


class TCPConnection(ConnectionBase):
    def __init__(self, *args, **kwargs):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        super(TCPConnection, self).__init__(*args, **kwargs)

    def connect(self):
        self._sock.connect(self._addr)

    def getsockname(self):
        return self._sock.getsockname()

    def close(self):
        self._sock.close()
