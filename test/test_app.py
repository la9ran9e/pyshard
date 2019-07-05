import unittest

from pyshard.app import Pyshard
from master.master import Shards
from shard.client import ShardClient

OK = 1


def mock_shard_client(v):
    variants = [
        f'{{"type": "", "response": {OK}}}',
        '{"type": "error", "response": "test error"}'
    ]
    def _monkey_execute(self, method, *args, **kwargs):
            return variants[v]

    MonkeyShardClient = type('MonkeyShardClient', ShardClient.__bases__, 
                             dict(ShardClient.__dict__))
    MonkeyShardClient._execute = _monkey_execute

    return MonkeyShardClient


class TestPyshard(unittest.TestCase):
    _shard_client_class = mock_shard_client(0)

    def setUp(self):
        test_addrs = [('127.0.0.1', 5050)]
        shards = Shards(*test_addrs, shard_client_class=self._shard_client_class)
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
            self.assertEqual(self.pyshard.write(doc[1], doc[2]), OK,       
                             f'could not write document: {doc}')

    def test_read(self):
        self.assertEqual(self.pyshard.read('test'), OK,
                         'could not read document')

    def test_pop(self):
        self.assertEqual(self.pyshard.pop('test'), OK,
                         'could not pop document')

    def test_remove(self):
        self.assertEqual(self.pyshard.remove('test'), OK,
                         'could not remove document')


class TestBadPyshard(TestPyshard):
    _shard_client_class = mock_shard_client(1)

    def test_write(self):
        self.assertEqual(self.pyshard.write('test', 'test doc'), 0,       
                         f'unexpected response')

    def test_read(self):
        self.assertEqual(self.pyshard.read('test'), None,       
                         f'unexpected response')

    def test_pop(self):
        self.assertEqual(self.pyshard.pop('test'), None,       
                         f'unexpected response')

    def test_remove(self):
        self.assertEqual(self.pyshard.remove('test'), 0,       
                         f'unexpected response')
