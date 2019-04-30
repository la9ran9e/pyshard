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


class InternalMethods:
	def init_shard(self, *args, **kwargs):
		if self._shard:
			return

		self._shard = Shard(*args, **kwargs)
		print('shard:', self._shard)


class ShardServer(Server, InternalMethods):
	def __init__(self, host, port, **kwargs):
		self._addr = (host, port)
		self._shard = None

		super(ShardServer, self).__init__(**kwargs)

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
		method = self._get_method(command)
		if not method:
			return

		ret = method(*args, **kwargs)

		return ret

	def _get_method(self, command):
		meth = getattr(self._shard, command, None)
		if not meth:
			meth = self._get_internal_method(command)

		return meth

	def _get_internal_method(self, command):
		meth = getattr(self, command, None)
		print(meth)

		return meth
