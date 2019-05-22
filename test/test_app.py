import unittest

from pyshard.app import Pyshard
from master.master import Shards
from shard.client import ShardClient

OK_size = 1


def _monkey_execute(self, method, *args, **kwargs):
		return '{"type": "", "response": 1}'


MonkeyShardClient = ShardClient
MonkeyShardClient._execute = _monkey_execute


class TestPyshard(unittest.TestCase):
	def setUp(self):
		test_addrs = [('127.0.0.1', 5050)]
		shards = Shards(*test_addrs, shard_client_class=MonkeyShardClient)
		self.pyshard = Pyshard(shards)

	def test_write(self):
		doc_cases = [
			(tuple, 'test-1', ('test',)),
			(str, 'test0', 'test'),
			(int, 'test1', 1),
			(float, 'test2', 1.0),
			(dict, 'test3', {'test': 'test'}),
			(list, 'test4', ['test', 'test'])

		]
		for doc in doc_cases:
			self.assertEqual(self.pyshard.write(doc[1], doc[2]), OK_size,		
							 f'could not write document: {doc}')
