import unittest

from pyshard.storage import InMemoryStorage
from pyshard.storage.errors import IndexNotFoundError


class TestInMemoryStorage(unittest.TestCase):
    def setUp(self):
        self.storage = InMemoryStorage()

    def _create_index(self, index):
        # check IndexNotFoundError
        with self.assertRaises(IndexNotFoundError) as context:
            self.storage.read(index, 'test')

            self.assertTrue(index in context)

        self.storage.create_index(index)

        # check index is created
        self.assertTrue(index in self.storage.indexes)

    def test_write_and_read(self):
        index = 'test'
        key = 'test_key'
        value = 'test_value'

        self._create_index(index)

        self.storage.write(index, key, value)
        self.assertEqual(self.storage.read(index, key), value)

    def test_read_not_exists(self):
        index = 'test'
        key = 'test_key'

        self._create_index(index)

        self.assertIsNone(self.storage.read(index, key))

    def test_write_to_index_not_exists(self):
        index = 'test'
        key = 'test_key'
        value = 'test_value'

        with self.assertRaises(IndexNotFoundError) as context:
            self.storage.write(index, key, value)

            self.assertTrue(index in context)

    def test_remove_and_read(self):
        index = 'test'
        key = 'test_key'
        value = 'test_value'

        self._create_index(index)

        self.storage.write(index, key, value)
        self.assertEqual(self.storage.pop(index, key), value)
        self.assertIsNone(self.storage.read(index, key))

    def test_pop_and_read(self):
        index = 'test'
        key = 'test_key'
        value = 'test_value'

        self._create_index(index)

        self.storage.write(index, key, value)
        self.storage.pop(index, key)
        self.assertIsNone(self.storage.read(index, key))

    def test_drop_index(self):
        index = 'test'

        self._create_index(index)

        self.storage.drop_index(index)
        self.assertTrue(index not in self.storage.indexes)
