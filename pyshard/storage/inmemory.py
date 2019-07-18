from .base import BaseStorage
from .errors import IndexNotFoundError, IndexExistsError


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
        if key in collection:
            return 0
        collection[key] = record

    def pop(self, index, key):
        collection = self._get_index(index)
        return collection.pop(key, None)

    def remove(self, index, key):
        collection = self._get_index(index)
        del collection[key]

    def create_index(self, index):
        if index in self._storage:
            raise IndexExistsError(index)
        self._storage[index] = dict()

    def drop_index(self, index):
        del self._storage[index]

    def values(self):
        for index in self.indexes:
            for value in self.index_values(index):
                yield value

    def index_values(self, index):
        collection = self._get_index(index)
        for key in collection:
            yield collection[key]

    @property
    def empty(self):
        for index in self.indexes:
            if self._storage[index]:
                return False
        return True

    def _get_index(self, index):
        if index not in self._storage:
            raise IndexNotFoundError(index)

        return self._storage[index]
