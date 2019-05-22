import sys
from shard.server import ShardServer

def get_port():
	if len(sys.argv) > 1:
		return int(sys.argv[1])

	else:
		return 5050

if __name__ == '__main__':
	p = ShardServer('127.0.0.1', get_port(), buffer_size=8)
	p._do_run()
