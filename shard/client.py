from core.typing import (Addr, Key, Hash, Doc, Offset)
from core.client import ClientABC, ClientBase


def mkpipe(addr: Addr, **kwargs) -> ClientABC:
    """
    Create instance of Client to connect to another shard

    :param addr: host and port to connect
    :param kwargs: params for Client instance
    :return: Client instance
    """
    return ShardClient(*addr, **kwargs)


class ShardClient(ClientBase):
    def write(self, key: Key, hash_: Hash, doc: Doc) -> Offset:
        record = {"record": doc, "hash_": hash_}
        response = self._deserialize(self._execute("write", key, **record))

        return self._handle_response(response)

    def read(self, key: Key):
        response = self._deserialize(self._execute("read", key))

        return self._handle_response(response)

    def pop(self, key: Key):
        response = self._deserialize(self._execute("pop", key))

        return self._handle_response(response)

    def remove(self, key: Key):
        response = self._deserialize(self._execute("remove", key))

        return self._handle_response(response)

    def open_pipe(self, host, port):
        response = self._deserialize(self._execute("open_pipe", (host, port)))

        return self._handle_response(response)

    def close_pipe(self):
        response = self._deserialize(self._execute("close_pipe"))

        return self._handle_response(response)

    def reloc(self, key, addr):
        response = self._deserialize(self._execute("reloc", key, addr))

        return self._handle_response(response)

    def get_stat(self):
        response = self._deserialize(self._execute("get_stat"))

        return self._handle_response(response)

    def _execute(self, method, *args, **kwargs):
        payload = {'endpoint': method,
                   'args': args,
                   'kwargs': kwargs}

        self._conn.send(self._serialize(payload))
        return self._conn.recv()
