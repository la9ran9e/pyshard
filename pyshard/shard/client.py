from ..core.typing import (Addr, Key, Hash, Doc, Offset)
from ..core.client import ClientABC, ClientBase


def mkpipe(addr: Addr, **kwargs) -> ClientABC:
    """
    Create instance of Client to connect to another shard

    :param addr: host and port to connect
    :param kwargs: params for Client instance
    :return: Client instance
    """
    return ShardClient(*addr, **kwargs)


class ShardClient(ClientBase):
    def write(self, index, key: Key, hash_: Hash, doc: Doc) -> Offset:
        record = {"record": doc, "hash_": hash_}
        return self._execute("write", index, key, **record)

    def has(self, index, key: Key):
        return self._execute("has", index, key)

    def read(self, index, key: Key):
        return self._execute("read", index, key)

    def pop(self, index, key: Key):
        return self._execute("pop", index, key)

    def remove(self, index, key: Key):
        return self._execute("remove", index, key)

    def open_pipe(self, host, port):
        return self._execute("open_pipe", (host, port))

    def close_pipe(self):
        return self._execute("close_pipe")

    def reloc(self, index, key, addr):
        return self._execute("reloc", index, key, addr)

    def get_stat(self):
        return self._execute("get_stat")

    def lock_shard(self):
        return self._execute("lock_shard")

    def release_shard(self):
        return self._execute("release_shard")

    def change_role(self, role, token=None):
        return self._execute("change_role", self.getsockname(), role, token=token)

    def set_start(self, value):
        return self._execute("set_start", value)

    def set_end(self, value):
        return self._execute("set_end", value)

    def update_distr(self):
        return self._execute("update_distr")

    def create_index(self, index):
        return self._execute("create_index", index)

    def drop_index(self, index):
        return self._execute("drop_index", index)

    def keys(self, index):  # TODO: bulk operation
        return self._execute("keys", index)

    def set_maxsize(self, size):
        return self._execute("set_maxsize", size)

    @property
    def name(self):
        return self._execute("get_name")

    @name.setter
    def name(self, name):
        self._execute("set_name", name)
