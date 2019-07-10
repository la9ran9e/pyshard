import sys
import pdb
from pyshard.app import Pyshard

app = Pyshard(bootstrap_server=('127.0.0.1', int(sys.argv[1])))
pdb.set_trace()
