from .base import BaseStorage
from .errors import IndexNotFoundError


class InMemoryStorage(BaseStorage):
    def __init__(self):
        self._storage = dict()

    @property
    def indexes(self):
        return self._storage.keys()

    def read(self, index, key):
        collection = self._get_index(index)
        return collection.get(key)

    def write(self, index, key, record):
        collection = self._get_index(index)
        collection[key] = record

    def pop(self, index, key):
        collection = self._get_index(index)
        return collection.pop(key, None)

    def remove(self, index, key):
        collection = self._get_index(index)
        del collection[key]

    def create_index(self, index):
        self._storage[index] = dict()

    def drop_index(self, index):
        del self._storage[index]

    def _get_index(self, index):
        if index not in self._storage:
            raise IndexNotFoundError(index)

        return self._storage[index]
