import abc
from typing import Union

from ..master.master import Master, _Shards
from ..master.client import MasterClient
from ..shard.client import ShardClient
from ..core.client import ClientError
from ..core.typing import Key, Doc, Hash


class AbstractResult(abc.ABC):
    @abc.abstractmethod
    def result(self) -> Union[int, Doc]: ...
    @abc.abstractmethod
    def hash(self) -> Hash: ...
    @abc.abstractmethod
    def __iter__(self): ...


class PyshardABC(abc.ABC):
    @abc.abstractmethod
    def write(self, index, key: Key, doc: Doc) -> AbstractResult: ...
    @abc.abstractmethod
    def read(self, index, key: Key) -> AbstractResult: ...
    @abc.abstractmethod
    def pop(self, index, key: Key) -> AbstractResult: ...
    @abc.abstractmethod
    def remove(self, index, key: Key) -> AbstractResult: ...
    @abc.abstractmethod
    def create_index(self, index): ...


def _map_shards(bootstrap_client, **kwargs):
    shard_map = {}
    map_ = bootstrap_client.get_map()
    for bin, addr in map_.items():
        shard_map[float(bin)] = ShardClient(*addr, **kwargs)

    return _Shards(shard_map)


class Result(AbstractResult):
    def __init__(self, result, hash_):
        self._result = result
        self._hash = hash_

    @property
    def result(self):
        return self._result

    @property
    def hash(self):
        return self._hash

    def __iter__(self):
        yield from [self.result, self.hash]


class Pyshard(PyshardABC):
    def __init__(self, bootstrap_server, buffer_size=1024, master_class=Master,
                 **master_args):
        self._bootstrap_client = MasterClient(*bootstrap_server, buffer_size=buffer_size)
        shards = _map_shards(self._bootstrap_client)  # TODO: add ShardClient kwargs
        self._master = master_class(shards=shards, **master_args)

    def write(self, index, key, doc) -> Result:
        hash_, shard = self._master.get_shard(index, key)
        try:
            offset = shard.write(index, key, hash_, doc)
        except ClientError as err:
            # log warning: err
            res = 0
        else:
            res = offset

        return Result(res, hash_)

    def has(self, index, key) -> Result:
        hash_, shard = self._master.get_shard(index, key)

        return Result(shard.has(index, key), hash_)

    def read(self, index, key) -> Result:
        hash_, shard = self._master.get_shard(index, key)
        try:
            doc = shard.read(index, key)
        except ClientError as err:
            # log warning: err
            res = None
        else:
            res = doc

        return Result(res, hash_)
        
    def pop(self, index, key) -> Result:
        hash_, shard = self._master.get_shard(index, key)
        try:
            doc = shard.pop(index, key)
        except ClientError as err:
            # log warning: err
            res = None
        else:
            res = doc

        return Result(res, hash_)

    def remove(self, index, key) -> Result:
        hash_, shard = self._master.get_shard(index, key)
        try:
            offset = shard.remove(index, key)
        except ClientError as err:
            # log warning: err
            res = 0
        else:
            res = offset

        return Result(res, hash_)

    def create_index(self, index):
        self._master.create_index(index)

    def drop_index(self, index):
        self._master.drop_index(index)

    def keys(self, index):
        for shard in self._master.shards:
            for key in shard.keys(index):
                yield key

    def close(self):
        self._bootstrap_client.close()
        self._master.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
