from collections import defaultdict

from .client import ShardClient
from ..utils import get_size


class Shard:
    def __init__(self, start, end, storage_class=dict, max_size=1024, bins_num=5,
                 buffer_size=1024,
                 **storage_kwargs):
        self._empty = True
        self.storage = storage_class(**storage_kwargs)
        self._buffer_size = buffer_size

        self.size = 0
        self.max_size = max_size
        self._start = start
        self._end = end
        self._bins_num = bins_num
        self._bin_step = self.estimate_bin_step()
        self._distr = defaultdict(int)

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
        return len(self.storage) == 0

    @property
    def free_mem(self):
        return self.max_size - self.size

    def write(self, key, hash_, record):
        item_size = get_size(record)
        if self.size + item_size > self.max_size:
            raise MemoryError(f'Wow! Such data! So big!')

        if key in self.storage:
            return 0

        doc = {'hash_': hash_, 'record': record}

        self.storage[key] = doc

        self.size += item_size

        bin_ = self._get_bin(hash_)
        self.distr[bin_] += 1

        return item_size

    def read(self, key):
        return self.storage.get(key, None)

    def pop(self, key):
        doc = self.storage.pop(key, None)
        if doc is None:
            return

        item_size = get_size(doc['record'])
        self.size -= item_size

        bin_ = self._get_bin(doc['hash_'])
        self.distr[bin_] -= 1

        return doc

    def remove(self, key):
        doc = self.storage.pop(key, None)
        if doc is None:
            return 0

        item_size = get_size(doc['record'])
        self.size -= item_size

        bin_ = self._get_bin(doc['hash_'])
        self.distr[bin_] -= 1

        return item_size

    def reloc(self, key, pipe: ShardClient):
        # relocates item from remote shard
        item = pipe.pop(key)

        if item:
            return self.write(key, **item)
        else:
            return 0

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

    def __getattr__(self, attr):
        return getattr(self.storage, attr)
