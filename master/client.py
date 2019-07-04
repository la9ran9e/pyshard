from core.client import ClientBase
from typing import Union, List, Any


Key = Union[int, float, str]

class MasterClient(ClientBase):
    def __init__(self, shards, *args, **kwargs):
        self._shards = shards
        
        super(MasterClient, self).__init__(*args, **kwargs)

    def get_shard(self, key) -> ShardClient: ...
