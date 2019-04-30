import hashlib
import bisect

from shard.client import ShardClient as ShardBase


def _normalize_number(num, boundary):
    # Normalizes between 0 and 1
    return float(num % boundary)/boundary


def _hash_key(key, method, boundary):
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


class _Nodes(list):
	def getfree(self):
		for node in self:
			if not node.acquired:
				return node


class Master:
	_shard = Shard

	def __init__(self, *nodes, shards_num=2, method='md5'):
		self._method = method
		self._bin_step = 1/shards_num
		self._bins = _make_bins(shards_num)
		self._shards = self._make_shards()
		self._nodes = _Nodes(*nodes)

	def _make_shards(self, size=1024):
		shards = dict()

		for bin_, node in zip(self._bins, self._nodes):
			next_bin = bin_+self._bin_step
			shard = self._shard(node, start=bin_, end=next_bin, max_size=size)
			shard.connect()

			if not shard.ready:
				shard.init_shard()
			else:
				shard.restat()

			bins[bin_] = shard
		
		return shards

	def _get_bin(self, key):
		id_ = _hash_key(key, self._method, 1e7)
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

	def _do_distr(self, shard, tg_shard=None):
		if not tg_shard:
			for key, value in shard.values():
				id_ = value['hash']
				i = bisect.bisect_left(self._bins, id_)-1

				bin_ = self._bins[i]
				tg_shard = self._shards[bin_]

				tg_shard.reloc(key, shard.node)

		else:
			for key in shard.key():
				tg_shard.reloc(key, shard.node)

	def insert(self, bin_, node=None, size=1024):
		if not node:
			node = self._nodes.getfree()
			node.acquire()

		i = bisect.bisect_right(self._bins, bin_)
		left_bin = self._bins[i-1]
		right_bin = self._bins[i+1]
		self._bins[i:i] = [bin_]
		new_shard = self._shard(node, start=bin_, end=right_bin, max_size=size)
		new_shard.connect()
		left_shard = self._shards[left_bin]

		_to_move = [key for key, value in left_shard.items() if value['hash'] >= bin_]

		for key in _to_move:
			new_shard.reloc(key, left_shard.node)

		self._shards[bin_] = new_shard

	def remove(self, bin_):
		i = self._bins.index(bin_)
		del self._bins[i]
		tmp = self._shards[bin_]
		self._shards[bin_]
		left_bin = self._bins[i-1]
		tg_shard = self._shards[left_bin]
		self._do_distr(tmp, tg_shard)

		tmp.close()
		tmp.node.release()

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
