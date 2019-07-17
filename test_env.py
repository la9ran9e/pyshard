import os
import sys
import json
import time
import subprocess
import shlex


INTERPRETER = '/usr/bin/python3.7'


def main():
    mode = sys.argv[1]
    if mode == 'build':
        with open(sys.argv[2], 'r') as config_file:
            config = json.load(config_file)
        shard_pids = run_shard_servers(config['shards'])
        _wait_for(1.15)
        bootstrap_pid = run_bootstrap_server(config['bootstrap'])
        save_pids(*shard_pids, bootstrap_pid)
    elif mode == 'kill':
        pidfile_path = sys.argv[2]
        with open(pidfile_path, 'r') as pidfile:
            pids = parse_pidfile(pidfile)
        for pid in pids:
            try:
                os.kill(pid, 2)
            except ProcessLookupError:
                pass

        os.remove(pidfile_path)


def run_shard_servers(shards_config):
    processes = []
    for shard_conf in shards_config:
        host, port = shard_conf['host'], shard_conf['port']
        proc = _mkserver('test_server', host, port)
        processes.append(proc.pid)

    return processes


def run_bootstrap_server(bootstrap_config):
    host, port = bootstrap_config['host'], bootstrap_config['port']
    proc = _mkserver('test_bootstrap_server', host, port)
    return proc.pid


def _wait_for(delay):
    time.sleep(delay)


def _mkserver(module, host, port):
    logfile_name = f"{module}_{host}:{port}.log"
    logfile = open(logfile_name, 'w')
    cmd = shlex.split(f'{INTERPRETER} {module}.py {host} {port}')
    proc = subprocess.Popen(cmd, stdout=logfile, stderr=logfile)

    return proc


def save_pids(*pids):
    with open('test_env.pid', 'w') as pidfile:
        for pid in pids:
            pidfile.write(f'{pid}\n')


def parse_pidfile(pidfile):
    pids = []
    for line in pidfile:
        pids.append(int(line.rstrip()))

    return pids


if __name__ == '__main__':
    main()
