import abc
from typing import Union

from ..master.master import Master, _Shards
from ..master.client import MasterClient
from ..shard.client import ShardClient
from ..core.client import ClientError

Key = Union[int, float, str]
Doc = Union[int, float, str, dict, list, tuple]


class PyshardABC(abc.ABC):
    @abc.abstractmethod
    def write(self, key: Key, doc: Doc) -> int: ...
    @abc.abstractmethod
    def read(self, key: Key) -> Doc: ...
    @abc.abstractmethod
    def pop(self, key: Key) -> Doc: ...
    @abc.abstractmethod
    def remove(self, key: Key) -> int: ...


def _map_shards(bootstrap_client, **kwargs):
    shard_map = {}
    map_ = bootstrap_client.get_map()
    for bin, addr in map_.items():
        shard_map[float(bin)] = ShardClient(*addr, **kwargs)

    return _Shards(shard_map)


class Pyshard(PyshardABC):
    def __init__(self, bootstrap_server, buffer_size=1024, master_class=Master,
                 **master_args):
        self._bootstrap_client = MasterClient(*bootstrap_server, buffer_size=buffer_size)
        shards = _map_shards(self._bootstrap_client)  # TODO: add ShardClient kwargs
        self._master = master_class(shards=shards, **master_args)

    def write(self, key, doc):
        hash_, shard = self._master.get_shard(key)
        try:
            offset = shard.write(key, hash_, doc)
        except ClientError as err:
            # log warning: err
            return 0
        else:
            return offset

    def read(self, key):
        _, shard = self._master.get_shard(key)
        try:
            doc = shard.read(key)
        except ClientError as err:
            # log warning: err
            return
        else:
            return doc
        
    def pop(self, key):
        _, shard = self._master.get_shard(key)
        try:
            doc = shard.pop(key)
        except ClientError as err:
            # log warning: err
            return
        else:
            return doc

    def remove(self, key):
        _, shard = self._master.get_shard(key)
        try:
            offset = shard.remove(key)
        except ClientError as err:
            # log warning: err
            return 0
        else:
            return offset
