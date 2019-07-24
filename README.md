# Pyshard

[![Build Status](https://travis-ci.org/la9ran9e/pyshard.svg?branch=master)](https://travis-ci.org/la9ran9e/pyshard)
[![Code Coverage Status](https://codecov.io/gh/la9ran9e/pyshard/branch/master/graph/badge.svg)](https://codecov.io/gh/la9ran9e/pyshard)
[![PyPI](https://img.shields.io/pypi/v/pyshard.svg)](https://pypi.org/project/pyshard/)

Pyshard is a complete distributed key-value data storage 
written in Python using only standard library tools.
Pyshard's using hash based sharding method. It means 
that shard of value you write will be selected in accordance to
key hash (regards to [lgiordani/pyshard](https://github.com/lgiordani/pyshard)). 
This project is experimental and should be used in another 
project [pdx](https://github.com/la9ran9e/pdx) - distributed web indexing service.

## Installation

```bash
pip install pyshard
```

## Quick start
### Bootstrap
To run 'hello world' service you need started up shard servers. For example:

```python
# test_server.py

import sys
import asyncio
from pyshard import ShardServer

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    server = ShardServer(host=sys.argv[1], port=int(sys.argv[2]), start=.0, end=1.0)
    try:
        loop.run_until_complete(server._do_run())
    finally:
        loop.close()
```
```bash
python test_server.py localhost 5050 & \
python test_server.py localhost 5051
```

After servers started up you should start bootstrap server to map shards. 
Now bootstrap server needs config file with shard's markers:

```json
{
  "shards": [
    {
      "name": "shard0-0.5",
      "start": 0.0,
      "end": 0.5,
      "size": 1024,
      "host": "127.0.0.1",
      "port": 5050
    },
    {
      "name": "shard0.5-1",
      "start": 0.5,
      "end": 1.0,
      "size": 1024,
      "host": "127.0.0.1",
      "port": 5051
    }
  ]
}
```
Every shard has next parameters:
`name` - unique string name of shard,
`start` and `end` - numeric limits of key hash,
`size` - memory limit for this shard,
`host` and `port` - shard address.
`start` and `end` limit means that this shard will store values with key hash in range `[start, end]`.
 
```python
# test_bootstrap_server.py

import asyncio

from pyshard import BootstrapServer

from pyshard.settings import settings


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    server = BootstrapServer(*settings.BOOTSTRAP_SERVER, config_path='config_example.json',
                             buffer_size=1024, loop=loop)
    try:
        loop.run_until_complete(server._do_run())
    finally:
        loop.close()

```

```bash
python test_bootstrap_server.py
```

Now shards have got configurations from bootstrap service and ready.

### App


```python
>>> from pyshard import Pyshard
>>> from pyshard.settings import settings
>>> 
>>> app = Pyshard(bootstrap_server=settings.BOOTSTRAP_SERVER)
>>> app.create_index('test_index')
>>> app.write(index='test_index', key='test', doc='hello world')
60
>>> app.read(index='test_index', key='test')
{'hash_': 0.1671936, 'record': 'hello world'}
>>> app.write('test_index', 'test1', {'hello': 'world'})
54
>>> app.read('test_index', 'test')
{'hash_': 0.8204544, 'record': {'hello': 'world'}}
>>> app.pop('test_index', 'test1')
{'hash_': 0.8204544, 'record': {'hello': 'world'}}
```

### Utilities

Since version 0.2.0 Pyshard has several console utilities. They are made to simplify some operations like `cat` or massive write.


Let's make file with data. Row format: `{key}|{value}`:

```bash
printf '1|test\n2|{"test": "test"}\n3|42\n4|0.9\n' > test_write.txt
```
We can add this rows to storage using `pyshard write` command.
```bash

cat test_write.txt | pyshard write test_index --force

```
`--force` oprion for creating index `test_index` if it does not exist


So let's `cat` storage with index `test_index`:
```bash
pyshard cat test_index
```
Command will log results to stdout:
```
2|{"hash_": 0.2258304, "record": {"test": "test"}}
3|{"hash_": 0.1904896, "record": 42}
1|{"hash_": 0.8102784, "record": "test"}
4|{"hash_": 0.7252864, "record": 0.9}
```


## TODO
* Index (data tables equivalent)
* Connection id for shard servers (now it is an address)
* App utils (`pyshard read`, `pyshard write`)
* Nice run methods for services
* Makefile
