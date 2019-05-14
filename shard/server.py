import json

from .protocol import Protocol
from .shard import Shard


class Server(Protocol):
	def _do_run(self):
		self._sock.bind(self._addr)
		self._sock.listen(1)

		while True:
			conn, addr = self._sock.accept()
			while True:
				try:
					msg = self._recvall(conn)
				except RuntimeError:
					print('close connection')
					conn.close()
					break
				except Exception as exc:
					print(exc)
					conn.close()
					break

				ret = self._on_recieved(msg)

				self._sendall(conn, ret)

	def _on_recieved(self, msg):
		print(f'recieved: {msg}')


class InternalAttrs:
	def init_shard(self, *args, **kwargs):
		if self._shard:
			return

		self._shard = Shard(*args, **kwargs)
		print('shard:', self._shard)


class ShardServer(Server, InternalAttrs):
	_methods = frozenset(['init_shard', 'write', 'read', 'pop', 'remove'])
	_properties = frozenset([])

	def __init__(self, host, port, **kwargs):
		self._addr = (host, port)
		self._shard = None

		super(ShardServer, self).__init__(**kwargs)

	@property
	def ready(self):
		return self._shard is not None

	def _on_recieved(self, msg):
		print(f'recieved: {msg}')
		command, args, kwargs = self._parse(msg)

		ret = self._execute(command, *args, **kwargs)

		return ret

	def _parse(self, msg):
		parts = msg.split('\t')
		command = parts[0]
		body = json.loads(parts[1])

		return command, body['args'], body['kwargs']

	def _execute(self, command, *args, **kwargs):
		attr = self._get_attr(command)
		
		if not attr:
			return

		if command in self._methods:
			ret = attr(*args, **kwargs)
		else:
			return

		return ret

	def _get_attr(self, command):
		attr = getattr(self._shard, command, None)
		if not attr:
			attr = self._get_internal_attr(command)

		return attr

	def _get_internal_attr(self, command):
		attr = getattr(self, command, None)

		return attr
