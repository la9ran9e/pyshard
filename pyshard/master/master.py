import hashlib
import bisect
import json
from contextlib import contextmanager

from ..core.server import ServerBase
from ..shard.client import ShardClient


def _normalize_number(num, boundary):
    # Normalizes between 0 and 1
    return float(num % boundary)/boundary


def _hash_key(key, method, boundary):
    hash_function = getattr(hashlib, method)
    hashed_key = int(hash_function(str(key).encode()).hexdigest(), 16)

    return _normalize_number(hashed_key, boundary)


def _make_bins(num):
    bin_step = 1.0/num

    bins = []

    bin_ = 0.0
    step = 0
    while step < num:
        bins.append(bin_)
        bin_ += bin_step
        step += 1

    return bins


import abc
from typing import Union, List, Tuple


Key = Union[int, float, str]
Addr = Tuple[str, int]
Hash = float
Bin = float


def _mark_up(length: int) -> list:
    bin_step = 1.0/length

    bins = []

    bin_ = 0.0
    step = 0
    while step < length:
        bins.append(bin_)
        bin_ += bin_step
        step += 1

    return bins


def _map(class_: type, bins: List[float], addrs: List[Addr], **kwargs):
    return {bin_: class_(*addr, **kwargs) for bin_, addr in zip(bins, addrs)}


class Shards:
    def __init__(self, *addrs, shard_client_class=ShardClient, **shard_kwargs):
        self._bins = _mark_up(len(addrs))
        self._shard_map = _map(shard_client_class, 
                               self._bins, addrs, 
                               **shard_kwargs)
        self._shards = self._shard_map.values()

    @property
    def bins(self):
        return self._bins

    def __getattr__(self, item):
        return getattr(self._shard_map, item)

    def __getitem__(self, item):
        return self._shard_map.__getitem__(item)


class MasterABC(abc.ABC):
    @abc.abstractmethod
    def get_shard(self, key: Key) -> Tuple[Hash, ShardClient]: ...


class Master(MasterABC):
    def __init__(self, shards: dict, hash_method: str='md5'):
        self._shards = shards
        self._hash_method = hash_method
    
    def get_shard(self, key):
        bin_, hash_ = self._get_bin(key)
        shard = self._shards[bin_]

        return hash_, shard

    def _get_bin(self, key: Key) -> Tuple[Bin, Hash]:
        bins = self._shards.bins
        hash_ = _hash_key(key, self._hash_method, 1e7)
        index = bisect.bisect_left(bins, hash_)-1
        bin_ = bins[index]

        return bin_, hash_

# class Master:
#   _shard = Shard

#   def __init__(self, *nodes, shards_num=2, method='md5'):
#       self._method = method
#       self._bin_step = 1/shards_num
#       self._bins = _make_bins(shards_num)
#       self._shards = self._make_shards()
#       self._nodes = nodes

#   def _make_shards(self, size=1024):
#       shards = dict()

#       for bin_, node in zip(self._bins, self._nodes):
#           next_bin = bin_+self._bin_step
#           shard = self._shard(*node, start=bin_, end=next_bin, max_size=size)
#           shard.connect()

#           if not shard.ready:
#               shard.init_shard()
#           else:
#               shard.restat()

#           bins[bin_] = shard
        
#       return shards

#   def _get_bin(self, key):
#       id_ = _hash_key(key, self._method, 1e7)
#       i = bisect.bisect_left(self._bins, id_)-1

#       return self._bins[i], id_

#   def get_shard(self, key):
#       bin_, hash_key_ = self._get_bin(key)
#       return hash_key_, self._shards[bin_]

#   def split(self, increase_num, size=1024):
#       num = len(self._shards)+increase_num
#       self._bins = _make_bins(num)
#       old_shards = self._shards
#       self._bin_step = 1/num
#       self._shards = self._make_shards(size=size)
#       for shard in old_shards.values():
#           self._do_distr(shard)

#   def _do_distr(self, shard, tg_shard=None):
#       if not tg_shard:
#           for key, value in shard.values():
#               id_ = value['hash']
#               i = bisect.bisect_left(self._bins, id_)-1

#               bin_ = self._bins[i]
#               tg_shard = self._shards[bin_]

#               tg_shard.reloc(key, shard.node)

#       else:
#           for key in shard.key():
#               tg_shard.reloc(key, shard.node)

#   def insert(self, bin_, node=None, size=1024):
#       if not node:
#           node = self._nodes.getfree()
#           node.acquire()

#       i = bisect.bisect_right(self._bins, bin_)
#       left_bin = self._bins[i-1]
#       right_bin = self._bins[i+1]
#       self._bins[i:i] = [bin_]
#       new_shard = self._shard(node, start=bin_, end=right_bin, max_size=size)
#       new_shard.connect()
#       left_shard = self._shards[left_bin]

#       _to_move = [key for key, value in left_shard.items() if value['hash'] >= bin_]

#       for key in _to_move:
#           new_shard.reloc(key, left_shard.node)

#       self._shards[bin_] = new_shard

#   def remove(self, bin_):
#       i = self._bins.index(bin_)
#       del self._bins[i]
#       tmp = self._shards[bin_]
#       self._shards[bin_]
#       left_bin = self._bins[i-1]
#       tg_shard = self._shards[left_bin]
#       self._do_distr(tmp, tg_shard)

#       tmp.close()
#       tmp.node.release()

#   def rebalance(self, shard):
#       ...

#   @property
#   def stat(self):
#       stat = dict()

#       for shard_id, shard in sorted(self._shards.items()):
#           stat[shard_id] = {
#               'total memory': shard.max_size,
#               'free memory': shard.free_mem,
#               'distribution': dict(shard.distr)

#           }

#       return stat


def _bootstrap(conf_path=None, *args, **kwargs):
    master_token = kwargs.pop('token', None)
    config = _get_config(conf_path)
    shards = _mkshards(config['shards'], *args, **kwargs)
    shards.get_master_role(master_token)
    with shards.lock():
        _mark_shards(shards, config['shards'])

    return shards


def _get_config(conf_path=None):
    if conf_path:
        config_file = open(conf_path, mode='r')
    else:
        raise NotImplementedError()

    config = json.load(config_file)

    shards = config['shards']

    # TODO: check names existence and uniqueness

    if _is_marked(shards):
        _check_markers(shards)

    return config


def _is_marked(shards):
    l = len(shards)
    shards = [shard for shard in shards if shard.get('start') is not None
              and shard.get('end') is not None]
    if len(shards) < l:
        raise Exception("All or no one shard must be marked")
    if not shards:
        return False

    return True


def _check_markers(shards):
    shards.sort(key=lambda x: x['start'])
    mem_end = 0.0
    for shard in shards:
        start = shard['start']
        end = shard['end']
        if end <= start:
            raise Exception("Shard end must be greater than it\'s start")
        if mem_end != start:
            raise Exception("Shard start must equal to previous shard end or 0.0")
        mem_end = end


class _Shards(dict):
    def __init__(self, *args, **kwargs):
        super(_Shards, self).__init__(*args, **kwargs)
        self._bins = sorted(self.keys())

    @property
    def bins(self):
        return self._bins

    def get_master_role(self, token=None):
        for shard in self.values():
            shard.change_role('master', token)

    @contextmanager
    def lock(self):
        for shard in self.values():
            shard.lock_shard()
        try:
            yield
        finally:
            for shard in self.values():
                shard.release_shard()

    def __setitem__(self, key, value):
        super(_Shards, self).__setitem__(key, value)
        bisect.insort_right(self._bins, key)


def _mkshards(shards_conf, *args, **kwargs):
    shards = _Shards()
    for shard in shards_conf:
        start = shard['start']
        host, port = shard['host'], shard['port']
        shards[start] = ShardClient(host, port, *args, **kwargs)

    return shards


def _mark_shards(shards, shards_conf):
    for shard, shard_conf in zip(shards.values(), shards_conf):  # TODO: remove values method
        start, end = shard_conf['start'], shard_conf['end']
        shard.set_start(start)
        shard.set_end(end)
        shard.update_distr()


class _Server(ServerBase): ...


class BootstrapServer(_Server):
    def __init__(self, *args, config_path=None, master=Master, hash_method='md5',
                 **kwargs):  # TODO: add bootstrap options
        self._shards = _bootstrap(config_path)
        self._master = master(shards=self._shards, hash_method=hash_method)

        super(BootstrapServer, self).__init__(*args, **kwargs)

    @_Server.endpoint('get_map')
    async def get_map(self):
        return {bin_: shard.addr for bin_, shard in self._shards.items()}

    @_Server.endpoint('get_shard')
    async def get_shard(self, key):
        hash_, shard = self._master.get_shard(key)
        return hash_, shard.addr