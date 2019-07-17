from typing import Union, List, Any
from ..core.client import ClientBase


Key = Union[int, float, str]

class MasterClient(ClientBase):
    def __init__(self, *args, **kwargs):
        
        super(MasterClient, self).__init__(*args, **kwargs)

    def get_shard(self, key):
        response = self._deserialize(self._execute("get_shard", key))

        return self._handle_response(response)

    def get_map(self):
        response = self._deserialize(self._execute("get_map"))

        return self._handle_response(response)
