from shard.client import ShardClient

if __name__ == '__main__':
	c = ShardClient('127.0.0.1', 5050, buffer_size=3)
	c.connect()
	# c.init_shard(.0, .5)
	print(c.pop('a'))
	c.init_shard(.0, .5)
	print(c.write('a', .01, 'a'))
	print(c.read('a'))
	# print(c.pop('a'))

	c.close()