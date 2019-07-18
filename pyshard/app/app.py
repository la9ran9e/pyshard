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
    def write(self, index, key: Key, doc: Doc) -> int: ...
    @abc.abstractmethod
    def read(self, index, key: Key) -> Doc: ...
    @abc.abstractmethod
    def pop(self, index, key: Key) -> Doc: ...
    @abc.abstractmethod
    def remove(self, index, key: Key) -> int: ...
    @abc.abstractmethod
    def create_index(self, index): ...


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

    def write(self, index, key, doc):
        hash_, shard = self._master.get_shard(index, key)
        try:
            offset = shard.write(index, key, hash_, doc)
        except ClientError as err:
            # log warning: err
            return 0
        else:
            return offset

    def read(self, index, key):
        _, shard = self._master.get_shard(index, key)
        try:
            doc = shard.read(index, key)
        except ClientError as err:
            # log warning: err
            return
        else:
            return doc
        
    def pop(self, index, key):
        _, shard = self._master.get_shard(index, key)
        try:
            doc = shard.pop(index, key)
        except ClientError as err:
            # log warning: err
            return
        else:
            return doc

    def remove(self, index, key):
        _, shard = self._master.get_shard(index, key)
        try:
            offset = shard.remove(index, key)
        except ClientError as err:
            # log warning: err
            return 0
        else:
            return offset

    def create_index(self, index):
        self._master.create_index(index)
