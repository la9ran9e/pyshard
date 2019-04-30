import sys
from collections import defaultdict


def _get_size(obj):
	if isinstance(obj, dict):
		size = sum((_get_size(v) for v in obj.values()))
	else:
		size = sys.getsizeof(obj)

	return size


class Shard:
	def __init__(self, storage_class=dict, start=None, end=None, max_size=1024, bins_num=5,
				 **storage_kwargs):
		self._empty = True
		self.storage = storage_class(**storage_kwargs)
		self.size = 0
		self.max_size = max_size
		self._start = start
		self._end = end

		self._bins_num = bins_num
		self._bin_step = self.estimate_bin_step()

		self.distr = defaultdict(int)

	@property
	def start(self):
		return self._start

	@property
	def end(self):
		return self._end

	def estimate_bin_step(self):
		if self._start is not None and self._end is not None:
			self._bin_step = (self._end - self._start) / self._bins_num
		else:
			self._bin_step = None

		return self._bin_step

	@start.setter
	def start(self, value):
		self._start = value

	@end.setter
	def end(self, value):
		self._end = value

	def _get_bin(self, hash):
		steps = (hash - self._start) // self._bin_step
		bin_ = self._start + (self._bin_step * steps)

		return bin_

	@property
	def empty(self):
		return len(self.storage) == 0

	@property
	def free_mem(self):
		return self.max_size - self.size

	def write(self, key, hash, record):
		item_size = _get_size(record)
		if self.size + item_size > self.max_size:
			raise MemoryError(f'Wow! Such data! So big!')

		if key in self.storage:
			return 0

		doc = {'hash': hash, 'record': record}

		self.storage[key] = doc

		self.size += item_size

		bin_ = self._get_bin(hash)
		self.distr[bin_] += 1

		return item_size

	def read(self, key):
		return self.storage.get(key, None)

	def pop(self, key):
		record = self.storage.pop(key, None)
		if record is None:
			return 

		item_size = _get_size(record)
		self.size -= item_size

		hash = record['hash']
		bin_ = self._get_bin(hash)
		self.distr[bin_] -= 1

		return record

	def remove(self, key):
		record = self.storage.pop(key, None)
		if record is None:
			return 0

		item_size = _get_size(record)
		self.size -= item_size

		bin_ = self._get_bin(hash)
		self.distr[bin_] -= 1

		return item_size

	def __getattr__(self, attr):
		return getattr(self.storage, attr)
