import abc
from typing import Union, List, Tuple

from master.master import MasterABC, Master, Shards
from shard.client import ShardClientError

Key = Union[int, float, str]
Doc = Union[int, float, str, dict, list, tuple]


class PyshardABC(abc.ABC):
    def __init__(self, shards: Shards, master_class: MasterABC=Master, **master_args):
        self._master = master_class(shards, **master_args)

        super(PyshardABC, self).__init__()

    @abc.abstractmethod
    def write(self, key: Key, doc: Doc) -> int: ...
    @abc.abstractmethod
    def read(self, key: Key) -> Doc: ...
    @abc.abstractmethod
    def pop(self, key: Key) -> Doc: ...
    @abc.abstractmethod
    def remove(self, key: Key) -> int: ...


class Pyshard(PyshardABC):
    def write(self, key, doc):
        hash_, shard = self._master.get_shard(key)
        try:
            offset = shard.write(key, hash_, doc)
        except ShardClientError as err:
            # log warning: err
            return 0
        else:
            return offset

    def read(self, key):
        ...
    def pop(self, key):
        ...
    def remove(self, key):
        ...
