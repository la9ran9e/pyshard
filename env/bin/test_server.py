import sys
import asyncio
import logging.config

from pyshard import ShardServer

# create logger
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('pyshard')

loop = asyncio.get_event_loop()

if __name__ == '__main__':
    server = ShardServer(host=sys.argv[1], port=int(sys.argv[2]), start=.0, end=.1)
    try:
        loop.run_until_complete(server._do_run())
    finally:
        loop.close()
