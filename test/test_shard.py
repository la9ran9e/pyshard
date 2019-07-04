import unittest

from shard.shard import Shard


START = .0
END = .1
KEY = 'test-key'
HASH = .01



class TestShard(unittest.TestCase):
	def setUp(self):
		self.shard = Shard(START, END)

	def test_write(self):
		record = 'test'

		self.assertEqual(self.shard.write(KEY, HASH, record), self.shard.size)

	def test_read(self):
		record = 'test'
		self.shard.write(KEY, HASH, record)

		self.assertEqual(self.shard.read(KEY), {'hash': HASH, 'record': record})

	def test_remove(self):
		record = 'test'

		self.assertEqual(self.shard.write(KEY, HASH, record), self.shard.remove(KEY))

	def test_pop(self):
		record = 'test'
		self.shard.write(KEY, HASH, record)

		self.assertEqual(self.shard.pop(KEY), {'hash': HASH, 'record': record})
		self.assertEqual(self.shard.size, 0)

	def test_reloc(self):
		pipe = self.shard.pipe(REMOTE_SHARD_ADDR)
		self.shard.reloc(KEY, pipe)
