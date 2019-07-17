import unittest

from pyshard import Pyshard
from pyshard.utils import get_size
from pyshard.settings import settings


class TestCommands(unittest.TestCase):
    TYPE_CASES = [
        ('test0', 1),
        ('test1', 1.1),
        ('test2', 'test'),
        ('test3', {'test': 'test'}),
        # ('test4', ['test0', 'test1'])
    ]

    @classmethod
    def setUpClass(cls):
        cls.app = Pyshard(bootstrap_server=settings.BOOTSTRAP_SERVER)

    def test_types(self):
        for key, doc in self.TYPE_CASES:
            self.assertEqual(self.app.write(key, doc), get_size(doc),
                             f'couldn\t write key={key}, doc={doc}')
            self.assertEqual(self.app.remove(key), get_size(doc),
                             f'couldn\t remove key={key}, doc={doc}')

    def test_write_and_read(self):
        key = 'test_key'
        doc = 'test_record'
        self.assertEqual(self.app.write(key, doc), get_size(doc),
                         f'couldn\t write key={key}, doc={doc}')
        self.assertEqual(self.app.read(key)['record'], doc,
                         f'couldn\t read key={key}, doc={doc}')

        self.assertEqual(self.app.remove(key), get_size(doc),
                         f'couldn\t remove key={key}, doc={doc}')

    def test_read_not_existing(self):
        key = 'test_key'
        self.app.remove(key)
        self.assertEqual(self.app.read(key), None, f'couldn\t read key={key}')

    def test_write_duplicate(self):
        key = 'test_key'
        doc = 'test_record'
        self.assertEqual(self.app.write(key, doc), get_size(doc),
                         f'couldn\t write key={key}, doc={doc}')
        self.assertEqual(self.app.write(key, doc), 0,
                         f'couldn\t write key={key}, doc={doc}')

        self.assertEqual(self.app.remove(key), get_size(doc),
                         f'couldn\t remove key={key}, doc={doc}')

    def test_write_and_pop(self):
        key = 'test_key'
        doc = 'test_record'
        self.assertEqual(self.app.write(key, doc), get_size(doc),
                         f'couldn\t write key={key}, doc={doc}')
        self.assertEqual(self.app.pop(key)['record'], doc,
                         f'couldn\'t populate key={key}, doc={doc}')

    def test_pop_not_existing(self):
        key = 'test_key'
        self.assertEqual(self.app.pop(key), None, f'couldn\'t populate key={key}')
