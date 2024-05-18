import logging
import time

from threading import Thread

import click

from pyudpbd.blkdev import IBlockDevice, MemoryBlockDevice, FileBlockDevice
from pyudpbd.server import BlockDeviceServer
from pyudpbd.unitutils import HumanBytes


def print_block_device_status_forever(blkdev: IBlockDevice, interval: float):
    logger = logging.getLogger('pyudpbd.blkdev')
    while True:
        read_bytes, written_bytes = blkdev.status()
        logger.info(
            f'Read: {HumanBytes.format(read_bytes, metric=True)}, '
            f'Written: {HumanBytes.format(written_bytes, metric=True)}'
        )
        time.sleep(interval)


@click.command()
@click.option('--path', type=str, help='Path of block device', required=True)
@click.option('--sector-size', type=int, default=512, help='Sector size of block device')
@click.option('--read-only', '-ro', is_flag=True, default=False, help='Read-only mode')
@click.option('--test-mode', is_flag=True, default=False, help='Use test mode')
def main(
        path: str,
        sector_size: int,
        read_only: bool,
        test_mode: bool
):
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('pyudpbd.blkdev').setLevel(logging.INFO)
    logging.getLogger('pyudpbd.server').setLevel(logging.INFO)

    if test_mode:
        blkdev = MemoryBlockDevice(path, sector_size)
    else:
        blkdev = FileBlockDevice(path, sector_size, read_only)
    server = BlockDeviceServer(blkdev)

    Thread(target=print_block_device_status_forever, args=(blkdev, 10), daemon=True).start()

    try:
        server.serve(timeout=1)
    finally:
        server.close()
        blkdev.close()


if __name__ == '__main__':
    main()
