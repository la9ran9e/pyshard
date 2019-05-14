from shard.server import ShardServer


if __name__ == '__main__':
	p = ShardServer('127.0.0.1', 5050, buffer_size=8)
	p._do_run()
