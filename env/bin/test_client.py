import pdb
import sys
from pyshard.shard.client import ShardClient

if __name__ == '__main__':
    c = ShardClient('127.0.0.1', int(sys.argv[1]), buffer_size=3)
    pdb.set_trace()
    
