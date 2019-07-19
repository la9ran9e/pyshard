class BaseStorage:
    def read(self, index, key): ...
    def write(self, index, key, record): ...
    def pop(self, index, key): ...
    def remove(self, index, key): ...

    def create_index(self, index): ...
    def drop_index(self, index): ...

    def values(self): ...
    def index_values(self, index): ...

    def empty(self): ...

    def _get_index(self, index): ...

    def keys(self, index): ...
