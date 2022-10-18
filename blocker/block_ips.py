import time

from clickhouse_api import get_clickhouse_url_generator, \
    IPAddressCount, get_ip_addresses
from typing import List
import subprocess


def block_ip_addresses(ip_structs: List[IPAddressCount]):
    ips = list(map(lambda struct: struct[0], ip_structs))
    commands = list(map(lambda ip: f'ipset -A blacklist {ip}', ips))
    command = ' && '.join(commands)
    subprocess.Popen('ipset -F blacklist', shell=True)
    subprocess.Popen(command, shell=True)


def main():
    ch_url_generator = get_clickhouse_url_generator()
    while True:
        ip_addresses = get_ip_addresses(ch_url_generator)
        if ip_addresses:
            block_ip_addresses(ip_addresses)
        time.sleep(5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        ...
