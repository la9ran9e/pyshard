import sys

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
	def __init__(self, max_size=1024):
		self._empty = True
		self.storage = dict()
		self.size = 0
		self.max_size = max_size

	@property
	def empty(self):
		return len(self.storage) == 0

	@property
	def left(self):
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

		return item_size

	def read(self, key):
		return self.storage.get(key, None)

	def pop(self, key):
		record = self.storage.pop(key, None)
		item_size = _get_size(record)
		self.size -= item_size

		return record

	def remove(self, key):
		record = self.storage.pop(key, None)
		if record is None:
			return 0

		item_size = _get_size(record)
		self.size -= item_size

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
		self._bins = _make_bins(shards_num)
		self._shards = self._make_shards()

	def _make_shards(self, size=1024):
		return {bin_: Shard(max_size=size) for bin_ in self._bins}

	def _get_bin_id(self, key):
		id_ = hash_key(key, self._method, 1e7)
		i = bisect.bisect_left(self._bins, id_)-1

		return self._bins[i], id_

	def get_shard(self, key):
		id_, hash_key_ = self._get_bin_id(key)
		return hash_key_, self._shards[id_]

	def split(self, increase_num):
		self._bins = _make_bins(len(self._shards)+increase_num)
		old_shards = self._shards
		self._shards = self._make_shards()
		for shard in old_shards.values():
			self._do_distr(shard)

	def _do_distr(self, shard):
		for key, value in shard.items():
			id_ = value['hash']
			i = bisect.bisect_left(self._bins, id_)-1
			bin_ = self._bins[i]

			self._shards[bin_].write(key, **value)

	def insert_right(self, bin_, size=1024):
		i = bisect.bisect_right(self._bins, bin_)
		left_bin = self._bins[i-1]
		self._bins[i:i] = [bin_]
		self._shards[bin_] = Shard(max_size=size)
		_to_move = [key for key, value in self._shards[left_bin].items() if value['hash'] >= bin_]
		for key in _to_move:
			item = self._shards[left_bin].pop(key)
			self._shards[bin_].write(key, **item)

	def rebalance(self, shard):
		...

	@property
	def stat(self):
		stat = dict()

		for shard_id, shard in sorted(self._shards.items()):
			stat[shard_id] = {
				'total memory': shard.max_size,
				'free memory': shard.left
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
print(app.stat)
for id_, shard in master._shards.items():
	print(id_, shard.storage)
app.split(2)
print(app.stat)
for id_, shard in master._shards.items():
	print(id_, shard.storage)
app.insert_right(.2)
print(app.stat)
for id_, shard in master._shards.items():
	print(id_, shard.storage)
