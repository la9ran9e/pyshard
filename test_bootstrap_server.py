import asyncio
import logging

from master.master import BootstrapServer

from settings import settings

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

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    server = BootstrapServer(*settings.BOOTSTRAP_SERVER, config_path='config_example.json',
                             buffer_size=1024, loop=loop)
    try:
        loop.run_until_complete(server._do_run())
    finally:
        loop.close()
