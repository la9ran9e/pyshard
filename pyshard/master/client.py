from typing import Union, List, Any
from ..core.client import ClientBase
from ..core.connect import AsyncTCPConnection

Key = Union[int, float, str]


class MasterClient(ClientBase):
    def get_shard(self, key):
        return self._execute("get_shard", key)

    def get_map(self):
        return self._execute("get_map")

    def stat(self):
        return self._execute("stat")

    def create_index(self, index):
        return self._execute("create_index", index)


class AsyncMasterClient(MasterClient):
    def __init__(self, host, port, transport_class=AsyncTCPConnection, **kwargs):
        super(AsyncMasterClient, self).__init__(host, port, transport_class, **kwargs)
