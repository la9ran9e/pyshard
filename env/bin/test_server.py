import sys
import asyncio
import logging.config

from pyshard import ShardServer

# create logger
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('pyshard')

loop = asyncio.get_event_loop()


if __name__ == '__main__':
    host, port = sys.argv[1], int(sys.argv[2])
    try:
        with ShardServer(host=host, port=port, start=.0, end=.1) as server:
            loop.run_until_complete(server._do_run())
    finally:
        loop.close()
