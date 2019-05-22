# import json

# from .protocol import Protocol


# class Connection(Protocol):
# 	def __init__(self, **kwargs):
# 		self.connected = False
# 		self.closed = False

# 		super(Connection, self).__init__(**kwargs)

# 	def connect(self):
# 		self._sock.connect(self._addr)
# 		self.connected = True

# 	def close(self):
# 		super(Connection, self).close()
# 		self.connected = False
# 		self.closed = True

# 	def reconnect(self):
# 		if self.connected:
# 			raise Exception('Connection still works!')

# 		self._sock = self._make_sock()
# 		self._sock.connect(self._addr)
# 		self.connected = True
# 		self.closed = False



# class ShardClient(Connection):
# 	def __init__(self, host, port, **kwargs):
# 		self._addr = (host, port)

# 		super(ShardClient, self).__init__(**kwargs)

# 	def _execute(self, command, args, kwargs):
# 		body = json.dumps({'args': args, 'kwargs': kwargs})
# 		msg = '\t'.join((command, body))

# 		self._sendall(self._sock, msg)

# 		resp_msg = self._recvall(self._sock)

# 		return self._retrive_response(resp_msg)

# 	def _retrive_response(self, resp_msg):

# 		response = json.loads(resp_msg)

# 		return response['message']

# 	def _get_property(self, property):
# 		self._sendall(self._sock, property)

# 		resp_msg = self._recvall(self._sock)

# 		return self._retrive_response(resp_msg)

# 	@property
# 	def ready(self):
# 		return self._get_property('ready')

# 	def init_shard(self, start, end, max_size=1024, bins_num=5):
# 		kwargs = {'start': start, 'end': end,
# 				  'max_size': max_size, 'bins_num': bins_num}

# 		self._execute('init_shard', [], kwargs)

# 	def write(self, key, hash, record):
# 		args = [key, hash, record]

# 		return self._execute('write', args, {})

# 	def read(self, key):
# 		args = [key]

# 		return self._execute('read', args, {})

# 	def pop(self, key):
# 		args = [key]

# 		return self._execute('pop', args, {})

# 	def remove(self, key):
# 		args = [key]

# 		return self._execute('remove', args, {})


# class _Pool:
# 	def __init__(self):
# 		self._released = dict()
# 		self._acquired = dict()

# 	def acquire(self, id):
# 		item = self._released.pop(id)
# 		self._do_acquire(item)
# 		self._acquired[id] = item

# 		return item

# 	def release(self, id):
# 		item = self._acquired.pop(id)
# 		self._do_release(item)
# 		self._released[id] = item

# 	def add(self, id, *args, **kwargs):
# 		raise NotImplementedError()

# 	def remove(self, id):
# 		raise NotImplementedError()


# class Pool(_Pool):
# 	def __init__(self, *args, client_class=ShardClient, **kwargs):
# 		self._client_class = client_class
		
# 		super(Pool, self).__init__(*args, **kwargs)

# 	def add(self, id, *args, **kwargs):
# 		self._released[id] = self._client_class(*args, **kwargs)

# 	def remove(self, id):
# 		del self._released[id]

# 	def _do_acquire(self, client):
# 		if client.closed:
# 			client.reconnect()
# 		else:
# 			client.connect()

# 	def _do_release(self, client):
# 		client.close()

# 	def get(self, id):
# 		return self._acquired.get(id)


import abc
from typing import Union, List, Any
import json

Key = Union[int, float, str]
Doc = Union[int, float, str, dict, list, tuple]
Hash = float
Payload = dict
JsonStr = str
Response = JsonStr

Offset = int


class SerialyzerError(Exception): ...


class Serialyzer:
	serialyzer = json

	@classmethod
	def dump(cls, dict_obj):
		return cls.serialyzer.dumps(dict_obj)

	@classmethod
	def load(cls, json_obj):
		return cls.serialyzer.loads(json_obj)


class ShardClientABC(abc.ABC):
	def __init__(self, host, port, serialyzer: type=Serialyzer):
		self._serialyzer = serialyzer
	@abc.abstractmethod
	def _serialize(self, payload: Payload) -> str: ...
	@abc.abstractmethod
	def _deserialize(self, response: str) -> dict: ...
	@abc.abstractmethod
	def _execute(self, method: str, *args, **kwargs) -> Response: ...
	@abc.abstractmethod
	def _handle_response(self, response: dict) -> Any: ...
	@abc.abstractmethod
	def write(self, key: Key, hash_: Hash, doc: Doc) -> Offset : ...


class ShardClientError(Exception): ...


class ShardClient(ShardClientABC):
	def _serialize(self, payload):
		return self._serialyzer.dump(payload)

	def _deserialize(self, response):
		return self._serialyzer.load(response)

	def _execute(self, method, *args, **kwargs):
		return '{"type": "", "response": ""}'

	def _handle_response(self, response):
		if response['type'] == 'error':
			err = response['response']
			raise ShardClientError(f'Couldn\'t execute: {err}')

		return response['response']

	def write(self, key, hash_, doc):
		payload = {"doc": doc, "hash": hash_}
		record = self._serialize(payload)
		response = self._deserialize(self._execute("write", record))

		return self._handle_response(response)
