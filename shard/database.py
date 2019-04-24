import sys
from collections import defaultdict


class Table(object):
	def __init__(self, storage):
		self._storage = storage


def _get_size(obj):
	if isinstance(obj, dict):
		size = sum((_get_size(v) for v in obj.values()))
	else:
		size = sys.getsizeof(obj)

	return size


def _sort_shards(shards):
	shards.sort(key=lambda shard: shard.left)


class Shard:
	def __init__(self, start=None, end=None, max_size=1024, bins_num=5):
		self._empty = True
		self.storage = dict()
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
			self._bin_step = (self._end-self._start)/self._bins_num
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
		steps = (hash - self._start)//self._bin_step
		bin_ = self._start + (self._bin_step*steps)

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


import hashlib
import bisect


def _normalize_number(num, boundary):
    # Normalizes between 0 and 1
    return float(num % boundary)/boundary


def hash_key(key, method, boundary):
    hash_function = getattr(hashlib, method)
    hashed_key = int(hash_function(str(key).encode()).hexdigest(), 16)

    return _normalize_number(hashed_key, boundary)

def _make_bins(num):
	bin_step = 1.0/num

	bins = []

	bin_ = 0.0
	step = 0
	while step < num:
		bins.append(bin_)
		bin_ += bin_step
		step += 1

	return bins


class Master:
	def __init__(self, shards_num=2, method='md5'):
		self._method = method
		self._bin_step = 1/shards_num
		self._bins = _make_bins(shards_num)
		self._shards = self._make_shards()

	def _make_shards(self, size=1024):
		bins = dict()

		for bin_ in self._bins:
			next_bin = bin_+self._bin_step
			bins[bin_] = Shard(start=bin_, end=next_bin, max_size=size)
		
		return bins

	def _get_bin(self, key):
		id_ = hash_key(key, self._method, 1e7)
		i = bisect.bisect_left(self._bins, id_)-1

		return self._bins[i], id_

	def get_shard(self, key):
		bin_, hash_key_ = self._get_bin(key)
		return hash_key_, self._shards[bin_]

	def split(self, increase_num, size=1024):
		num = len(self._shards)+increase_num
		self._bins = _make_bins(num)
		old_shards = self._shards
		self._bin_step = 1/num
		self._shards = self._make_shards(size=size)
		for shard in old_shards.values():
			self._do_distr(shard)

	def _do_distr(self, shard, target=None):
		if not target:
			for key, value in shard.items():
				id_ = value['hash']
				i = bisect.bisect_left(self._bins, id_)-1
				bin_ = self._bins[i]

				self._shards[bin_].write(key, **value)
		else:
			for key, value in shard.items():
				target.write(key, **value)

	def insert(self, bin_, size=1024):
		i = bisect.bisect_right(self._bins, bin_)
		left_bin = self._bins[i-1]
		right_bin = self._bins[i+1]
		self._bins[i:i] = [bin_]
		new_shard = Shard(start=bin_, end=right_bin, max_size=size)
		left_shard = self._shards[left_bin]

		_to_move = [key for key, value in left_shard.items() if value['hash'] >= bin_]
		for key in _to_move:
			item = left_shard.pop(key)
			new_shard.write(key, **item)

		self._shards[bin_] = new_shard

	def remove(self, bin_):
		i = self._bins.index(bin_)
		del self._bins[i]
		tmp = self._shards[bin_]
		del self._shards[bin_]
		left_bin = self._bins[i-1]
		target = self._shards[left_bin]
		self._do_distr(tmp, target)

	def rebalance(self, shard):
		...

	@property
	def stat(self):
		stat = dict()

		for shard_id, shard in sorted(self._shards.items()):
			stat[shard_id] = {
				'total memory': shard.max_size,
				'free memory': shard.free_mem,
				'distribution': dict(shard.distr)

			}

		return stat


class App:
	def __init__(self, master):
		self._master = master

	def write(self, key, item):
		hash_key_, shard = self._master.get_shard(key)
		try:
			size = shard.write(key, hash_key_, item)
		except MemoryError:
			size = 0

		return size

	def read(self, key):
		_, shard = self._master.get_shard(key)

		return shard.read(key)

	def __getattr__(self, attr):
		return getattr(self._master, attr)
	

import pprint

pp = pprint.PrettyPrinter()

master = Master()
app = App(master)
app.write('name1', 1)
print(app.stat)
app.write('name2', {'a': 'a'})
print(app.stat)
app.write('name3', 3)
print(app.stat)
app.write('name4', {'b': 'b'})
app.write('name5', {'b': 'b'})
app.write('name6', {'b': 'b'})
app.write('name7', {'b': 'b'})
app.write('name8', {'b': 'b'})
pp.pprint(app.stat)
app.split(2)
pp.pprint(app.stat)
app.insert(.2)
pp.pprint(app.stat)
app.remove(.2)
pp.pprint(app.stat)


