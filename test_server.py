import logging
import logging.config
import sys
import asyncio

from pyshard import ShardServer

# create logger
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('pyshard')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

loop = asyncio.get_event_loop()

server = ShardServer(host=sys.argv[1], port=int(sys.argv[2]), start=.0, end=.1)
loop.run_until_complete(server._do_run())
loop.close()
