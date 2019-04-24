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