import sys
import asyncio
import logging.config

from pyshard import BootstrapServer

# create logger
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('pyshard')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    server = BootstrapServer(host=sys.argv[1], port=int(sys.argv[2]), config_path='config_example.json',
                             buffer_size=1024, loop=loop)
    try:
        loop.run_until_complete(server._do_run())
    finally:
        loop.close()
