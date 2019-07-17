class Node:
	def __init__(self, addr):
		self.addr = addr
		self._acquired = False

	def acquire(self):
		self._acquired = True

	@property
	def acquired(self):
		return self._acquired

	def release(self):
		self._acquired = False
