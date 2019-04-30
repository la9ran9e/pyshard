import json

from .protocol import Protocol


class Connection(Protocol):
	def __init__(self, **kwargs):
		self.connectd = False

		super(Connection, self).__init__(**kwargs)

	def connect(self):
		self._sock.connect(self._addr)
		self.connected = True

	def close(self):
		super(Connection, self).close()
		self.connected = False

	def __getattr__(self, attr):
		return getattr(self._sock, attr)


class ShardClient(Connection):
	def __init__(self, host, port, **kwargs):
		self._addr = (host, port)

		super(ShardClient, self).__init__(**kwargs)

	def _execute(self, command, args, kwargs):
		body = json.dumps({'args': args, 'kwargs': kwargs})
		msg = '\t'.join((command, body))

		self._sendall(self._sock, msg)

		response = self._recvall(self._sock)

		return response

	def init_shard(self, start, end, max_size=1024, bins_num=5):
		kwargs = {'start': start, 'end': end,
				  'max_size': max_size, 'bins_num': bins_num}

		self._execute('init_shard', [], kwargs)

	def write(self, key, hash, record):
		args = [key, hash, record]

		return self._execute('write', args, {})

	def read(self, key):
		args = [key]

		return self._execute('read', args, {})

	def pop(self, key):
		args = [key]

		return self._execute('pop', args, {})

	def remove(self, key):
		args = [key]

		return self._execute('remove', args, {})
