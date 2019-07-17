import json
import asyncio
from collections import defaultdict
from struct import error as struct_error

import logging

from ..core.typing import Request, Response, rRequest, rResponse
from ..utils import to_bytes, from_bytes
from ..settings import settings

from .connect import AsyncProtocol, mksock


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

class ServerBase(AsyncProtocol):
    __routes__ = dict()
    __roles__ = set()
    __permissions__ = defaultdict(set)

    def __init__(self, host, port, buffer_size, loop,
                 serialize=json.dumps, deserialize=json.loads,
                 backlog=5):
        self.sock = mksock(host, port, backlog=backlog, mode='l')
        self._default_queue = asyncio.Queue(maxsize=buffer_size)
        self._master_queue = asyncio.Queue(maxsize=buffer_size // 2)
        self._master_group = 'master'
        self._token_storage = defaultdict(dict)
        self._channels = dict()

        self._proc_locker = asyncio.Lock()

        self._serialize = serialize
        self._deserialize = deserialize

        super(ServerBase, self).__init__(buffer_size, loop)

    async def _do_run(self):
        await asyncio.gather(self._worker(self._master_queue),
                             self._worker(self._default_queue),
                             self._main_loop())

    def _dispatch(self, endpoint):
        return self.__routes__[endpoint]

    @classmethod
    def endpoint(cls, path, permission_group=None):
        def _wrapper(method):
            if permission_group:
                cls.__roles__.add(permission_group)
                cls.__permissions__[path].add(permission_group)

            cls.__routes__[path] = method

        return _wrapper

    def _check_permission(self, chan, endpoint):
        if not self.__permissions__[endpoint]:
            return

        if chan.permission_group not in self.__permissions__[endpoint]:
            raise Exception("Permission denied")

    async def _dispatch_and_execute(self, chan, endpoint, *args, **kwargs):
        self._check_permission(chan, endpoint)
        async with self._proc_locker:
            return await self._dispatch(endpoint)(self, *args, **kwargs)

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
