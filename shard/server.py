from struct import error as struct_error
import json
import asyncio
from collections import defaultdict

import logging

from core.typing import Request, Response, rRequest, rResponse

from core.connect import AsyncProtocol, mksock
from .shard import Shard
from .client import mkpipe
from utils import to_bytes, from_bytes

from settings import settings


logger = logging.getLogger(__name__)

Kb = 1024


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
        self.token = from_bytes(rdata, self._codec)

    @property
    def sock(self):
        return self._chan[0]

    @property
    def addr(self):
        return self._chan[1]

    async def msg_iterator(self):
        while True:
            try:
                data = await self.do_recv(self.sock)
            except struct_error as err:
                logger.error(f'Couldn\'t unpack message: {err}')
            except RuntimeError as err:
                logger.warning(f'Addr={self.addr}: {err}')
                break
            except AssertionError as err:
                logger.warning(f'Addr={self.addr} send not enought data. {err}')
                break
            else:
                logger.debug(f'Received message from addr={self.addr}: {data}')
                yield from_bytes(data, self._codec)


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
    __roles__ = set()
    __permissions__ = defaultdict(set)

    def __init__(self, host, port, buffer_size, loop,
                 serialize=json.dumps, deserialize=json.loads,
                 backlog=5):
        self.sock = mksock(host, port, backlog=backlog, mode='l')
        self._default_queue = asyncio.Queue(maxsize=buffer_size)
        self._master_queue = asyncio.Queue(maxsize=buffer_size//2)
        self._master_group = 'master'
        self._token_storage = defaultdict(dict)
        self._channels = dict()

        # self._token_storage['master']['group'] = self._master_group

        self._shard_locked = False
        self._proc_locker = asyncio.Lock()

        self._serialize = serialize
        self._deserialize = deserialize

        super(Server, self).__init__(buffer_size, loop)

    def _dispatch(self, endpoint):
        return self.__routes__[endpoint]

    @classmethod
    def endpoint(cls, path, with_lock=True, permission_group=None):
        def wrapper(method):
            if permission_group:
                cls.__roles__.add(permission_group)
                cls.__permissions__[path].add(permission_group)
            
            async def method_with_lock(self, *args, **kwargs):
                if self._shard_locked:
                    raise Exception("Shard is locked")

                return await method(self, *args, **kwargs)

            cls.__routes__[path] = method_with_lock if with_lock else method

        return wrapper

    def _check_permission(self, chan, endpoint):
        if not self.__permissions__[endpoint]:
            return

        if chan.permission_group not in self.__permissions__[endpoint]:
            raise Exception("Permission denied")

    async def _dispatch_and_execute(self, chan, endpoint, *args, **kwargs):
        self._check_permission(chan, endpoint)
        async with self._proc_locker:
            return await self._dispatch(endpoint)(self, *args, **kwargs)
    
    async def _do_run(self):
        await asyncio.gather(self._worker(self._master_queue),
                             self._worker(self._default_queue),
                             self._main_loop())

    async def _worker(self, queue):
        while True:
            chan, endpoint, args, kwargs = await queue.get()
            try:
                rresp = await self._dispatch_and_execute(chan, endpoint, *args, **kwargs)
            except Exception as err:
                resp = self._handle_error_resp(err)
            else:
                resp = self._handle_success_resp(rresp)

            await self.do_send(to_bytes(resp, self._codec), chan.sock)

            queue.task_done()

    async def _main_loop(self):
        async for chan in self._channel_iterator():
            logger.debug(f"Connection accepted from: {chan.addr}")
            self._loop.create_task(self._handle_channel(chan))

        self.sock.close()

    async def _handle_channel(self, chan):
        async for msg in chan.msg_iterator():
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
        del self._channels[chan.addr]

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
        chan = await _Channel(sock, loop).connect()
        self._channels[chan.addr] = chan
        return chan

    def _parse_request(self, request: rRequest) -> Request:
        req = self._deserialize(request)

        return req['endpoint'], req.get('args', list()), req.get('kwargs', dict())

    def _handle_success_resp(self, rresp: rResponse) -> Response:
        resp = {"type": "success", "message": rresp}

        return self._serialize(resp)

    def _handle_error_resp(self, err: Exception) -> str:
        resp = {"type": "error", "message": err.args}

        return self._serialize(resp)


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

        self._pipe = mkpipe(*args, **kwargs)

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
        if self._shard_locked:
            raise Exception('Already locked')

        self._shard_locked = True

    @Server.endpoint('release_shard', with_lock=False, permission_group='master')
    async def release_shard(self):
        if not self._shard_locked:
            raise Exception('Shard is not locked')

        self._shard_locked = False

    @Server.endpoint('change_role')
    async def change_role(self, addr, role, token=None):
        addr = tuple(addr)
        if settings.AUTH and not token:
            raise Exception('Token is required')
        if role not in self.__roles__:
            raise Exception(f'Role {role!r} does not exists')
        try:
            chan = self._channels[addr]
        except KeyError:
            raise Exception(f'No such address={addr}')

        chan.permission_group = role

    @Server.endpoint('set_start', with_lock=False, permission_group='master')
    async def set_start(self, value):
        self._shard.start = value

    @Server.endpoint('set_end', with_lock=False, permission_group='master')
    async def set_start(self, value):
        self._shard.end = value