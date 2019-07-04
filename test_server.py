import logging
import sys
import asyncio

from shard.server import ShardServer

# create logger
logger = logging.getLogger('shard')
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

server = ShardServer(host='127.0.0.1', port=int(sys.argv[1]), start=.0, end=.1)
loop.run_until_complete(server._do_run())
loop.close()
