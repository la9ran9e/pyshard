import pdb
from pyshard.app import Pyshard

from settings import settings

app = Pyshard(bootstrap_server=settings.BOOTSTRAP_SERVER)
pdb.set_trace()
