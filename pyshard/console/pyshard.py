import sys
import argparse
import inspect
import json

from pyshard import Pyshard
from pyshard.core.client import ClientError
from pyshard.settings import settings


REGISTRY = dict()
SEPARATOR = '|'


def _register(name):
    def _wrapper(func):
        REGISTRY[name] = func

        return func

    return _wrapper


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('func', type=str)
    parser.add_argument('index', type=str)
    parser.add_argument('-b', '--bootstrap-server', type=str)
    parser.add_argument('--force', action='store_true')

    args = parser.parse_args()

    dispatch_and_execute(args.__dict__)


def dispatch_and_execute(args):
    func = REGISTRY[args.pop('func')]
    args = {key: value for key, value in args.items()
            if key in inspect.getfullargspec(func).args}
    func(**args)


@_register('cat')
def cat(index, bootstrap_server=None):
    bootstrap_server = _retrieve_bootstrap_server(bootstrap_server)

    with Pyshard(bootstrap_server=bootstrap_server) as app:
        for key in app.keys(index):
            doc = app.read(index, key).result
            sys.stdout.write(f'{key}{SEPARATOR}{json.dumps(doc)}\n')
            sys.stdout.flush()


def _process_doc(raw_doc):
    if raw_doc.startswith('{') and raw_doc.endswith('}'):
        doc = json.loads(raw_doc)
    elif raw_doc.isdigit():
        doc = int(raw_doc)
    elif isfloat(raw_doc):
        doc = float(raw_doc)
    else:
        doc = raw_doc

    return doc


def isfloat(raw_doc):
    parts = raw_doc.split('.')
    if len(parts) != 2:
        return False
    if not ((parts[0].isdigit() or not parts[0]) and parts[1].isdigit()):
        return False

    return True


@_register('write')
def write(index, bootstrap_server=None, force=False):
    bootstrap_server = _retrieve_bootstrap_server(bootstrap_server)

    with Pyshard(bootstrap_server=bootstrap_server) as app:
        if force:
            try:
                app.create_index(index)
            except ClientError:
                print(f"Index {index!r} already exists", file=sys.stderr)

        for line in sys.stdin:
            key, raw_doc = line.rstrip('\n').split(SEPARATOR)
            doc = _process_doc(raw_doc)
            app.write(index, key, doc)


def _retrieve_bootstrap_server(bootstrap_server):
    if bootstrap_server:
        bs = bootstrap_server.split(':')
        return [bs[0], int(bs)]
    else:
        return settings.BOOTSTRAP_SERVER


@_register('test_func')
def test_func(index, bootstrap_server):
    print(index, bootstrap_server)
