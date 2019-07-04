# import json

# from .protocol import Protocol
# from .shard import Shard


import socket
import struct
import json
import asyncio
from collections import defaultdict
from typing import Tuple, Any

import logging

from core.connect import AsyncProtocol
from .shard import Shard

from settings import settings


logger = logging.getLogger(__name__)

Kb = 1024
Codec = str


def _to_bytes(str_obj: str, codec: Codec) -> bytes:
    return bytes(str_obj, encoding=codec)


def _from_bytes(bytes_obj: bytes, codec: Codec) -> str:
    return bytes_obj.decode(codec)


class _Channel(AsyncProtocol):
    def __init__(self, sock, loop, buffer_size=1024):
        self._sock = sock
        self._loop = loop
        self._chan = None
        self.token = None
        self.permission_group = None

        super(_Channel, self).__init__(buffer_size, loop)

    async def connect(self):
        self._chan = await self._loop.sock_accept(self._sock)
        return self

    async def retrieve_token(self):
        rdata = await self.do_recv(self.sock)
        self.token = _from_bytes(rdata, self._codec)

    @property
    def sock(self):
        return self._chan[0]

    @property
    def addr(self):
        return self._chan[1]


def _auth(func):
    async def wrapper(self, *args, **kwargs):
        chan = await func(self, *args, **kwargs)
        await chan.retrieve_token()
        if chan.token in self._token_storage:
            chan.permission_group = self._token_storage[chan.token].get('group')
            return chan
        else:
            raise KeyError(f'Not authorized user token={chan.token}')

    return wrapper if settings.AUTH else func


class Server(AsyncProtocol):
    __routes__ = dict()
    __permissions__ = defaultdict(set)

    def __init__(self, host, port, buffer_size, loop, op_buffer_size=1000):
        self.sock = self._make_sock(host, port)
        self._default_queue = asyncio.Queue(maxsize=buffer_size)
        self._master_queue = asyncio.Queue(maxsize=buffer_size//2)
        self._master_group = 'master'
        self._token_storage = defaultdict(dict)

        # self._token_storage['master']['group'] = self._master_group

        self._shard_locked = False
        self._proc_locker = asyncio.Lock()

        super(Server, self).__init__(buffer_size, loop)

    @staticmethod
    def _make_sock(host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setblocking(False)
        sock.bind((host, port))
        sock.listen(10)

        return sock

    def _dispatch(self, endpoint):
        return self.__routes__[endpoint]

    @classmethod
    def endpoint(cls, path, with_lock=True, permission_group=None):
        def wrapper(method):
            cls.__routes__[path] = method
            if permission_group:
                cls.__permissions__[path].add(permission_group)
            
            async def method_with_lock(self, *args, **kwargs):
                if self._shard_locked:
                    raise Exception("Shard is locked")

                return await method(self, *args, **kwargs)

            return method_with_lock if with_lock else method
        return wrapper

    def _check_permission(self, chan, endpoint):
        if not self.__permissions__[endpoint]:
            return

        if chan.permission_group not in self.__permissions__[endpoint]:
            raise Exception("Permission denied")

    async def _dispatch_and_execute(self, chan, endpoint, *args, **kwargs):
        self._check_permission(chan, endpoint)
        with self._proc_locker:
            return await self._dispatch(endpoint)(self, *args, **kwargs)
    
    async def _do_run(self):
        await asyncio.gather(self._worker(self._master_queue),
                             self._worker(self._default_queue),
                             self._main_loop())

    async def _worker(self, queue):
        while True:
            chan, endpoint, args, kwargs = await queue.get()
            try:
                rresp = self._dispatch_and_execute(chan, endpoint, *args, **kwargs)
            except Exception as err:
                resp = self._handle_error_resp(err)
            else:
                resp = self._handle_success_resp(rresp)

            await self.do_send(_to_bytes(resp, self._codec), chan.sock)

            queue.task_done()

    async def _main_loop(self):
        async for chan in self._channel_iterator():
            logger.debug(f"Connection accepted from: {chan.addr}")
            self._loop.create_task(self._handle_channel(chan))

        self.sock.close()

    async def _handle_channel(self, chan):
        async for msg in self._msg_iterator(chan):
            try:
                endpoint, args, kwargs = self._parse_request(msg)
            except Exception as err:
                logger.warning(f"Couldn\'t parse message={msg!r}, addr={chan.addr} error: {err}")
                break

            if chan.permission_group == self._master_group:
                queue = self._master_queue
            else:
                queue = self._default_queue

            await queue.put((chan, endpoint, args, kwargs))

        chan.sock.close()

    async def _channel_iterator(self):
        while True:
            logger.debug("Waiting for connection...")
            try:
                yield await self._accept(self.sock, self._loop)
            except KeyError as err:
                logger.warning(err)
            except asyncio.TimeoutError:
                logger.warning('Couldn\'t recieve token from peer')

    @_auth
    async def _accept(self, sock, loop):
        return await _Channel(sock, loop).connect()

    async def _msg_iterator(self, channel):
        while True:
            try:
                data = await self.do_recv(channel.sock)
            except struct.error as err:
                logger.error(f'Couldn\'t unpack message: {err}')
            except RuntimeError as err:
                logger.warning(f'Addr={channel.addr}: {err}')
                break
            except AssertionError as err:
                logger.warning(f'Addr={channel.addr} send not enought data. {err}')
                break
            else:
                logger.debug(f'Received message from addr={channel.addr}: {data}')
                yield _from_bytes(data, self._codec)

    def _parse_request(self, request: str) -> Tuple[str, list, dict]:
        req = json.loads(request)

        return req['endpoint'], req.get('args', list()), req.get('kwargs', dict())

    def _handle_success_resp(self, rresp: Any) -> str:
        resp = {"type": "success", "message": rresp}

        return json.dumps(resp)

    def _handle_error_resp(self, err: Exception) -> str:
        resp = {"type": "error", "message": err.args}

        return json.dumps(resp)


class ShardServer(Server):
    def __init__(self, host, port, buffer_size=1024, loop=None, **shard_kwargs):
        self._shard = Shard(**shard_kwargs)
        self._pipe = None

        super(ShardServer, self).__init__(host, port, buffer_size, loop)

    @Server.endpoint('write')
    async def write(self, key, hash_, record):
        return self._shard.write(key, hash_, record)

    @Server.endpoint('read')
    async def read(self, key):
        return self._shard.read(key)

    @Server.endpoint('pop')
    async def pop(self, key):
        return self._shard.pop(key)

    @Server.endpoint('remove')
    async def remove(self, key):
        return self._shard.remove(key)

    @Server.endpoint('open_pipe')
    async def open_pipe(self, *args, **kwargs):
        if self._pipe:
            raise Exception(f'Pipe={self._pipe} already open.')

        self._pipe = self._shard.pipe(*args, **kwargs)

    @Server.endpoint('close_pipe')
    async def close_pipe(self):
        if not self._pipe:
            raise Exception('No working pipe.')

        self._pipe.close()
        self._pipe = None

    @Server.endpoint('reloc')
    async def reloc(self, key, addr: list):
        if not self._pipe:
            raise Exception('No working pipe.')
        if self._pipe.addr != tuple(addr):
            raise Exception(f'Wrong pipe. Exists: {self._pipe.addr}, got: {addr}')

        return self._shard.reloc(key, self._pipe)

    @Server.endpoint('get_stat')
    async def get_stat(self):
        return self._shard.get_stat()

    @Server.endpoint('lock_shard', with_lock=False, permission_group='master')
    async def lock_shard(self):
        if self._locked:
            raise Exception('Already locked')

        self._locked = True

    @Server.endpoint('release_shard', with_lock=False, permission_group='master')
    async def release_shard(self):
        if not self._locked:
            raise Exception('Shard is not locked')

        self._locked = False
