from collections import defaultdict

from ..storage import InMemoryStorage
from .client import ShardClient
from ..utils import get_size


class Shard:
    def __init__(self, start, end, storage_class=InMemoryStorage, max_size=1024, bins_num=5,
                 buffer_size=1024,
                 **storage_kwargs):
        self._name = None
        self._empty = True
        self.storage = storage_class(**storage_kwargs)
        self.storage.start()
        self._buffer_size = buffer_size

        self.size = 0
        self.max_size = max_size
        self._start = start
        self._end = end
        self._bins_num = bins_num
        self._bin_step = self.estimate_bin_step()
        self._distr = defaultdict(int)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def distr(self):
        return self._distr

    def update_distr(self):
        self._distr = defaultdict(int)
        for doc in self.storage.values():
            hash_ = doc['hash_']
            bin_ = self._get_bin(hash_)
            self._distr[bin_] += 1

    @property
    def start(self):
        return self._start

    @start.setter
    def start(self, value):
        assert isinstance(value, float)
        self._bin_step = self.estimate_bin_step()
        self._start = value

    @property
    def end(self):
        return self._end

    @end.setter
    def end(self, value):
        assert isinstance(value, float)
        self._bin_step = self.estimate_bin_step()
        self._end = value

    def estimate_bin_step(self):
        if self._start is not None and self._end is not None:
            bin_step = (self._end - self._start) / self._bins_num
        else:
            bin_step = None

        return bin_step

    def _get_bin(self, hash_):
        steps = (hash_ - self._start) // self._bin_step
        bin_ = self._start + (self._bin_step * steps)

        return bin_

    @property
    def empty(self):
        return self.storage.empty

    @property
    def free_mem(self):
        return self.max_size - self.size

    def write(self, index, key, hash_, record):
        item_size = get_size(record)
        if self.size + item_size > self.max_size:  # TODO replace memory control to storage
            raise MemoryError(f'Wow! Such data! So big!')

        doc = {'hash_': hash_, 'record': record}

        offset = self.storage.write(index, key, doc)
        if offset == 0:  # TODO replace memory control to storage
            return 0

        self.size += item_size

        bin_ = self._get_bin(hash_)
        self.distr[bin_] += 1

        return item_size

    def has(self, index, key):
        return self.storage.has(index, key)

    def read(self, index, key):
        return self.storage.read(index, key)

    def pop(self, index, key):
        doc = self.storage.pop(index, key)
        if doc is None:
            return

        item_size = get_size(doc['record'])
        self.size -= item_size

        bin_ = self._get_bin(doc['hash_'])
        self.distr[bin_] -= 1

        return doc

    def remove(self, index, key):
        doc = self.storage.pop(index, key)
        if doc is None:
            return 0

        item_size = get_size(doc['record'])
        self.size -= item_size

        bin_ = self._get_bin(doc['hash_'])
        self.distr[bin_] -= 1

        return item_size

    def reloc(self, index, key, pipe: ShardClient):
        # relocates item from remote shard
        item = pipe.pop(index, key)

        if item:
            return self.write(index, key, **item)
        else:
            return 0

    def create_index(self, index):
        self.storage.create_index(index)

    def keys(self, index):
        return self.storage.keys(index)

    def get_stat(self):
        stat = {
            'start': self.start,
            'end': self.end,
            'empty': self.empty,
            'max_size': self.max_size,
            'free_mem': self.free_mem,
            'distribution': dict(self.distr)
        }

        return stat

    def close(self):
        self.storage.stop()
