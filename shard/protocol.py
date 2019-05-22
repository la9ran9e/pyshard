from socket import socket, AF_INET, SOCK_STREAM
import struct


class Protocol:
	
	def __init__(self, buffer_size=1024):
		self._sock = self._make_sock()
		self._buffer_size = buffer_size
		self._prefix = struct.Struct(f'I')

	@staticmethod
	def _make_sock():
		sock = socket(AF_INET, SOCK_STREAM)

		return sock
	
	def _recvall(self, sock):
		data = b''
		total = 0

		prefix = sock.recv(self._prefix.size)
		if not prefix:
			raise RuntimeError('Connection was closed by peer')

		msg_len = self._prefix.unpack(prefix)[0]
		
		while total < msg_len:
			buff_size = min(msg_len - total, self._buffer_size)
			chunk = sock.recv(buff_size)
			print('chunk recieved:', chunk)

			data += chunk
			total += buff_size

		return self._unpack(data)

	def _unpack(self, bdata):
		data = bdata.decode('UTF-8')
		return data

	def _sendall(self, sock, msg):
		data = self._pack(msg)
		length = len(data)
		prefix = self._prefix.pack(length)
		data = prefix + data
	
		sock.sendall(data)

	def _pack(self, data):
		bdata = bytes(data, encoding='UTF-8')
		return bdata

	def close(self):
		self._sock.close()
