import json

import logging

from ..settings import settings
from ..core.server import ServerBase
from .shard import Shard
from .client import mkpipe


logger = logging.getLogger(__name__)


class _Server(ServerBase):
    def __init__(self, host, port, buffer_size, loop,
                 serialize=json.dumps, deserialize=json.loads,
                 backlog=5):
        self._shard_locked = False

        super(_Server, self).__init__(host, port, buffer_size, loop, serialize, deserialize, backlog)

    @classmethod
    def with_shard_lock(cls, method):
        async def method_with_lock(self, *args, **kwargs):
            if self._shard_locked:
                raise Exception("Shard is locked")

            return await method(self, *args, **kwargs)

        return method_with_lock


class ShardServer(_Server):
    def __init__(self, host, port, buffer_size=1024, loop=None, **shard_kwargs):
        self._shard = Shard(**shard_kwargs)
        self._pipe = None

        super(ShardServer, self).__init__(host, port, buffer_size, loop)

    @_Server.endpoint('write')
    @_Server.with_shard_lock
    async def write(self, index, key, hash_, record):
        return self._shard.write(index, key, hash_, record)

    @_Server.endpoint('has')
    @_Server.with_shard_lock
    async def has(self, index, key):
        return self._shard.has(index, key)

    @_Server.endpoint('read')
    @_Server.with_shard_lock
    async def read(self, index, key):
        return self._shard.read(index, key)

    @_Server.endpoint('pop')
    @_Server.with_shard_lock
    async def pop(self, index, key):
        return self._shard.pop(index, key)

    @_Server.endpoint('remove')
    @_Server.with_shard_lock
    async def remove(self, index, key):
        return self._shard.remove(index, key)

    @_Server.endpoint('open_pipe')
    @_Server.with_shard_lock
    async def open_pipe(self, *args, **kwargs):
        if self._pipe:
            raise Exception(f'Pipe={self._pipe} already open.')

        self._pipe = mkpipe(*args, **kwargs)

    @_Server.endpoint('close_pipe')
    @_Server.with_shard_lock
    async def close_pipe(self):
        if not self._pipe:
            raise Exception('No working pipe.')

        self._pipe.close()
        self._pipe = None

    @_Server.endpoint('reloc')
    @_Server.with_shard_lock
    async def reloc(self, index, key, addr: list):
        if not self._pipe:
            raise Exception('No working pipe.')
        if self._pipe.addr != tuple(addr):
            raise Exception(f'Wrong pipe. Exists: {self._pipe.addr}, got: {addr}')

        return self._shard.reloc(index, key, self._pipe)

    @_Server.endpoint('get_stat')
    @_Server.with_shard_lock
    async def get_stat(self):
        return self._shard.get_stat()

    @_Server.endpoint('lock_shard', permission_group='master')
    async def lock_shard(self):
        if self._shard_locked:
            raise Exception('Already locked')

        self._shard_locked = True

    @_Server.endpoint('release_shard', permission_group='master')
    async def release_shard(self):
        if not self._shard_locked:
            raise Exception('Shard is not locked')

        self._shard_locked = False

    @_Server.endpoint('change_role')
    @_Server.with_shard_lock
    async def change_role(self, addr, role, token=None):
        addr = tuple(addr)
        if settings.AUTH and not token:
            raise Exception('Token is required')
        print(self.__dict__)
        if role not in self._roles:
            raise Exception(f'Role {role!r} does not exists')
        try:
            chan = self._channels[addr]
        except KeyError:
            raise Exception(f'No such address={addr}')

        chan.permission_group = role

    @_Server.endpoint('set_start', permission_group='master')
    async def set_start(self, value):
        self._shard.start = value

    @_Server.endpoint('set_end', permission_group='master')
    async def set_end(self, value):
        self._shard.end = value

    @_Server.endpoint('update_distr', permission_group='master')
    async def update_distr(self):
        self._shard.update_distr()

    @_Server.endpoint('create_index')
    async def create_index(self, index):
        self._shard.create_index(index)

    @_Server.endpoint('keys')
    async def keys(self, index):
        return self._shard.keys(index)

    @_Server.endpoint('get_name')
    async def get_name(self):
        return self._shard.name

    @_Server.endpoint('set_name', permission_group='master')
    async def set_name(self, name):
        self._shard.name = name

    @_Server.endpoint('set_maxsize', permission_group='master')
    async def set_maxsize(self, size):
        assert size > self._shard.size, f'Can\'t apply {size}b. ' \
                                        f'Current size: {self._shard.size}b'

        self._shard.max_size = size

    def close(self):
        self._shard.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
