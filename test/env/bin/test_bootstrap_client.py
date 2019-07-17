import pdb
import sys
from pyshard import MasterClient

if __name__ == '__main__':
    c = MasterClient('127.0.0.1', int(sys.argv[1]), buffer_size=3)
    pdb.set_trace()