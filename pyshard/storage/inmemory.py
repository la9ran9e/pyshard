import os
import json

from .base import BaseStorage
from .errors import IndexNotFoundError, IndexExistsError


class InMemoryStorage(BaseStorage):
    def __init__(self, dump_filepath=None):
        self._storage = dict()
        self._dump_filepath = dump_filepath

    @property
    def indexes(self):
        return self._storage.keys()

    def has(self, index, key):
        collection = self._get_index(index)
        return key in collection

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

    def keys(self, index):
        collection = self._get_index(index)
        return list(collection.keys())

    def start(self):
        if not self._dump_filepath:
            return

        if os.path.exists(self._dump_filepath):
            with open(self._dump_filepath, 'r') as f:
                self._load_dump(f)

    def _load_dump(self, file):
        data = json.load(file)
        self._storage = data

    def stop(self):
        if not self._dump_filepath:
            return

        with open(self._dump_filepath, 'w') as f:
            self._dump(f)

    def _dump(self, file):
        json.dump(self._storage, file)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
