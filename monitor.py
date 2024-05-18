import json
import logging
import os
import time
import platform

from typing import Optional, Dict
from subprocess import Popen, PIPE

import click


if 'Linux' not in platform.system():
    raise RuntimeError('monitor service is only supported on Linux')


logging.basicConfig(level=logging.DEBUG)


def list_block_device_partitions(disk_only: bool = True) -> Dict:
    cmd = ['lsblk', '--json']
    p = Popen(cmd, stdout=PIPE)
    out, _ = p.communicate()
    try:
        objs = json.loads(out.decode('utf-8'))
        block_devices = objs.get('blockdevices', [])
        results = {}
        for blkdev_info in block_devices:
            if disk_only and blkdev_info.get('type') != 'disk':
                continue

            name = blkdev_info.get('name')
            children = blkdev_info.get('children', [])
            if not children:
                mountpoints = blkdev_info.get('mountpoints', [])
                results[name] = {
                    'parent': None,
                    'mountpoints': mountpoints,
                }
                continue

            for child_blkdev_info in children:
                child_name = child_blkdev_info.get('name')
                mountpoints = child_blkdev_info.get('mountpoints', [])
                results[child_name] = {
                    'parent': name,
                    'mountpoints': mountpoints,
                }
        return results
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logging.debug(f'failed to query block devices: {e}')
    return {}


def mount_block_device(blkdev_name: str, mountpoint: str, ro: bool = True) -> bool:
    blkdev_path = f'/dev/{blkdev_name}'
    cmd = ['mount', '-t', 'auto', '-o', 'ro' if ro else 'rw', blkdev_path, mountpoint]
    p = Popen(cmd, stdout=PIPE)
    p.communicate()
    return p.returncode == 0


def unmount_block_device(mountpoint: str) -> bool:
    cmd = ['umount', '-l', mountpoint]
    p = Popen(cmd, stdout=PIPE)
    p.communicate()
    return p.returncode == 0


def has_flag_in_block_device(
        blkdev_name: str,
        flag_name: str,
        mountpoint: Optional[str],
        base_path: str = '/tmp/pyudpbd/partitions'
) -> bool:
    blkdev_path = f'/dev/{blkdev_name}'
    temp_mountpoint = mountpoint if mountpoint else os.path.join(base_path, blkdev_name)

    try:
        if not mountpoint:
            os.makedirs(temp_mountpoint, exist_ok=True)
            if not mount_block_device(blkdev_name, temp_mountpoint):
                logging.warning(f'mount failed: {blkdev_path} -> {temp_mountpoint}')
                return False
        if os.path.exists(os.path.join(temp_mountpoint, flag_name)):
            return True
    finally:
        if not mountpoint:
            if not unmount_block_device(temp_mountpoint):
                logging.warning(f'unmount failed: {blkdev_path} -> {temp_mountpoint}')
    return False


def run_pyudpbd_server(blkdev_path: str, ro: bool = True, interpreter: str = 'python3') -> int:
    return os.system(f'{interpreter} server.py --path {blkdev_path} {"--read-only" if ro else ""}')


@click.command()
@click.option('--period', type=int, default=10, help='Period to check block devices')
@click.option('--read-only', is_flag=True, default=True, help='Open block device in read-only mode')
@click.option('--flag', type=str, default='pyudpbd', help='Flag file name to check')
@click.option('--interpreter', type=str, default='python3', help='Interpreter to run server')
def main(
        period: int,
        read_only: bool,
        flag: str,
        interpreter: str
):
    logging.info('server monitor is started')
    prev_partitions = set()

    while True:
        partition_infos = list_block_device_partitions()
        current_partitions = set(partition_infos)

        if prev_partitions != current_partitions:
            prev_partitions = current_partitions

            for blkdev_name, info in partition_infos.items():
                mountpoint = info.get('mountpoints', [None])[0]
                logging.info(f'searching flag in /dev/{blkdev_name}')

                if has_flag_in_block_device(blkdev_name, flag, mountpoint):
                    logging.info(f'found flag in /dev/{blkdev_name}')
                    run_pyudpbd_server(f'/dev/{blkdev_name}', ro=read_only, interpreter=interpreter)
                    logging.info(f'terminated.')
                    prev_partitions.clear()
                    break
        time.sleep(period)


if __name__ == '__main__':
    main()
