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


class Master:
	# TODO: command log and self.flush()
	DEFAULT_STORAGE = dict
	DEFAULT_SHARD_MIN_SIZE = 1024
	
	def __init__(self, shards, storage=None, *args, **kwargs):
		storage = storage if storage else self.DEFAULT_STORAGE
		self._shard_min_size = kwargs.pop('shard_min_size', self.DEFAULT_SHARD_MIN_SIZE)

		self._storage = storage(*args, **kwargs)

		self._shards = shards
		_sort_shards(self._shards)

		self._shards_changed = False

	@property
	def shards(self):
		return self
	

	def get_for_write(self, size):
		shard = self._shards[-1]
		if shard.left < size:
			raise MemoryError(f'No shard of such size. Maximum size: {shard.left}.')

		return shard

	def get_for_read(self, hash):
		idx = self._bisect_left(hash)
		return self._shards[idx]

	def add(self, shard):
		self._insort_left(shard)

	def _insort_left(self, shard):
	    lo = 0
	    hi = len(self._shards)
	    while lo < hi:
	        mid = (lo+hi)//2
	        if self._shards[mid].left < shard.left: lo = mid+1
	        else: hi = mid

	    self._shards.insert(lo, shard)

	def _bisect_left(self, hash):
		lo = 0
	    hi = len(self._shards)
	    while lo < hi:
	        mid = (lo+hi)//2
	        if self._shards[mid].end < hash: lo = mid+1
	        else: hi = mid

	    return lo

class Part:
	def __init__(self, max_size=1024):
		self.start: str = None
		self.end: str = None
		self._empty = True
		self.storage = dict()
		self.size = 0
		self.max_size = max_size

	@property
	def empty(self):
		if not self._empty and len(self.storage) == 0:
			self._empty = True
		
		return self._empty

	@property
	def left(self):
		return self.max_size - self.size
	
	def write(self, hash, record):
		if not self._empty:
			if self.end < hash:
				self.end = hash
			elif self.start > hash:
				self.start = hash
		else:
			self.start = self.end = hash
			self._empty = False

		if hash in self.storage:
			return 0

		self.storage[hash] = record
		
		item_size = _get_size(record)
		if self.size + item_size > self.max_size:
			raise MemoryError(f'Wow! Such data! So big!')

		self.size += item_size

		return item_size

	def read(self, hash):
		return self.storage.get(hash, None)

	def remove(self, hash):
		record = self.storage.pop(hash, None)
		if record is None:
			return 0

		item_size = _get_size(record)
		self.size -= item_size

		if self.empty:
			self.start = self.end = None
			return True

		if self.end == hash:
			self.end = max(self.storage.keys())
			
		elif self.start == hash:
			self.start = min(self.storage.keys())

		return item_size


sh = Shard()
item = 1
sh.write(str(item), item)
print(sh.start, sh.end)
item = 2
sh.write(str(item), item)
print(sh.start, sh.end)
item = 0
sh.write(str(item), item)
print(sh.start, sh.end)
sh.remove(str(item))
print(sh.start, sh.end)
item = {'a': 'a'}
sh.write('a', item)
print(sh.start, sh.end)