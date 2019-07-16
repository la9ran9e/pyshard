import os
import sys
import json
import subprocess


def main():
    mode = sys.argv[1]
    if mode == 'build':
        with open(sys.argv[2], 'r') as config_file:
            config = json.load(config_file)
        shard_pids = run_shard_servers(config['shards'])
        bootstrap_pid = run_bootstrap_server(config['bootstrap'])
        save_pids(*shard_pids, bootstrap_pid)
    elif mode == 'kill':
        pidfile_path = sys.argv[2]
        with open(pidfile_path, 'r') as pidfile:
            pids = parse_pidfile(pidfile)
        for pid in pids:
            os.kill(pid, 2)

        os.remove(pidfile_path)


def run_shard_servers(shards_config):
    processes = []
    interpreter = "python"
    for shard_conf in shards_config:
        host, port = shard_conf['host'], shard_conf['port']
        logfile_name = f"test_server_{host}:{port}.log"
        proc = subprocess.Popen(f'{interpreter} test_server.py {host} {port}',
                                shell=True)
        processes.append(proc.pid)

    return processes


def run_bootstrap_server(bootstrap_config):
    interpreter = "python"
    host, port = bootstrap_config['host'], bootstrap_config['port']
    logfile_name = f"test_bootstrap_server_{host}:{port}.log"
    proc = subprocess.Popen(f'{interpreter} test_bootstrap_server.py {host} {port}',
                            shell=True)
    return proc.pid


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
