import errno
import fcntl
import logging
import os
import platform
import shutil
import struct
from contextlib import suppress

import psutil

from abc import ABCMeta, abstractmethod
from typing import Tuple

from pyudpbd.unitutils import HumanBytes


logger = logging.getLogger('pyudpbd.blkdev')


class IBlockDevice(metaclass=ABCMeta):
    @property
    @abstractmethod
    def sector_size(self):
        pass

    @property
    @abstractmethod
    def sector_count(self):
        pass

    @abstractmethod
    def available(self) -> bool:
        pass

    @abstractmethod
    def status(self) -> Tuple[int, int]:
        pass

    @abstractmethod
    def seek(self, sector_offset: int) -> None:
        pass

    @abstractmethod
    def read(self, size: int) -> bytes:
        pass

    @abstractmethod
    def write(self, data: bytes) -> None:
        pass

    @abstractmethod
    def close(self):
        pass


class MemoryBlockDevice(IBlockDevice):
    # Allocate 16MB buffer by default to test block device
    DEFAULT_BLOCK_DEVICE_SIZE = 16 * 1024 * 1024

    def __init__(self, _: str, sector_size: int = 512):
        self._sector_size = sector_size
        self._sector_count = self.DEFAULT_BLOCK_DEVICE_SIZE // sector_size
        self._data = bytearray(self.DEFAULT_BLOCK_DEVICE_SIZE)
        self._total_read = 0
        self._total_written = 0
        self._position = 0
        self._valid = True

    @property
    def sector_size(self):
        return self._sector_size

    @property
    def sector_count(self):
        return self._sector_count

    def available(self) -> bool:
        return self._valid

    def status(self) -> Tuple[int, int]:
        return self._total_read, self._total_written

    def seek(self, sector_offset: int) -> None:
        logger.debug(f'seek. sector={self.sector_size}, offset={sector_offset}')
        offset = sector_offset * self._sector_size
        self._position = offset

    def read(self, size: int) -> bytes:
        logger.debug(f'read. size={size}')
        position = self._position
        self._position += size
        self._total_read += size
        return self._data[position:position + size]

    def write(self, data: bytes) -> None:
        logger.debug(f'write. size={len(data)}')
        position = self._position
        self._position += len(data)
        self._total_written += len(data)
        self._data[position:position + len(data)] = data

    def close(self):
        pass


class FileBlockDevice(IBlockDevice):
    @staticmethod
    def get_blkdev_size_linux(path) -> Tuple[int, int]:
        BLKGETSIZE64 = 0x80081272  # BLKGETSIZE64, result is bytes as unsigned 64-bit integer (uint64)
        buf = b' ' * 8
        with open(path) as dev:
            buf = fcntl.ioctl(dev.fileno(), BLKGETSIZE64, buf)
        return 1, struct.unpack('L', buf)[0]

    @staticmethod
    def get_blkdev_size_darwin(path) -> Tuple[int, int]:
        # TODO not working properly on macOS
        DKIOCGETBLOCKSIZE = 0x40046418
        DKIOCGETBLOCKCOUNT = 0x40046419

        with open(path) as dev:
            buf = b' ' * 8
            buf = fcntl.ioctl(dev.fileno(), DKIOCGETBLOCKSIZE, buf)
            sector_size = struct.unpack('L', buf)[0]
            buf = b' ' * 4
            buf = fcntl.ioctl(dev.fileno(), DKIOCGETBLOCKCOUNT, buf)
            sector_count = struct.unpack('L', buf)[0]
        return sector_size, sector_count

    @staticmethod
    def get_blkdev_size(path: str) -> Tuple[int, int]:
        platform_name = platform.system()
        if 'Linux' in platform_name:
            return FileBlockDevice.get_blkdev_size_linux(path)
        elif 'Darwin' in platform_name:
            return FileBlockDevice.get_blkdev_size_darwin(path)
        raise NotImplementedError(f'Unsupported platform: {platform_name}')

    @classmethod
    def open_block_device(cls, path: str, mode: int = os.O_RDONLY):
        if os.path.ismount(path) or os.path.isdir(path):
            # Opened by mount point or path (for Windows/macOS)
            partitions = psutil.disk_partitions()
            partitions_by_path = {p.mountpoint: p for p in partitions}
            partition = partitions_by_path.get(path)
            if partition is None:
                raise IOError(f'No mapped device: {path}')
            size = shutil.disk_usage(path).total
            fd = os.open(partition.device, mode)
            return fd, size

        # Opened by device file (for Linux/macOS)
        sector_size, sector_count = cls.get_blkdev_size(path)
        size = sector_size * sector_count
        fd = os.open(path, mode)
        return fd, size

    def __init__(self, path: str, sector_size: int = 512, ro: bool = False):
        try:
            fd, size = self.open_block_device(path, os.O_RDONLY if ro else os.O_RDWR)
        except OSError as e:
            logger.exception(e)
            logger.warning(f'failed to open block device {path} retry with read-only mode.')
            fd, size = self.open_block_device(path, os.O_RDONLY)
            ro = True

        self._fd = fd
        self._path = path
        self._fsize = size
        self._sector_size = sector_size
        self._sector_count = self._fsize // sector_size
        self._ro = ro
        self._total_read = 0
        self._total_written = 0

        logger.info(
            f'{path}, '
            f'size={HumanBytes.format(self._fsize, metric=False)}, '
            f'sectorSize={self._sector_size}, '
            f'sectorCount={self._sector_count}, '
            f'ro={ro}'
        )

    @property
    def sector_size(self):
        return self._sector_size

    @property
    def sector_count(self):
        return self._sector_count

    def available(self) -> bool:
        try:
            # TODO is good method?
            # Send invalid argument to check if the device is available
            # If block device is unplugged or down, it will raise ENODEV
            fcntl.ioctl(self._fd, -1)
        except OSError as e:
            err, _ = e.args
            if err == errno.ENODEV:
                logger.exception(e)
                return False
        return True

    def status(self) -> Tuple[int, int]:
        return self._total_read, self._total_written

    def seek(self, sector_offset: int) -> None:
        logger.debug(f'seek. sector={self.sector_size}, offset={sector_offset}')
        os.lseek(self._fd, self._sector_size * sector_offset, os.SEEK_SET)

    def read(self, size: int) -> bytes:
        logger.debug(f'read. size={size}')
        self._total_read += size
        return os.read(self._fd, size)

    def write(self, data: bytes) -> None:
        logger.debug(f'write. size={len(data)}')
        if self._ro:
            logger.warning('write. block device is read-only')
            return
        self._total_written += len(data)
        os.write(self._fd, data)

    def close(self):
        logger.debug('close.')
        with suppress(OSError):
            os.close(self._fd)
