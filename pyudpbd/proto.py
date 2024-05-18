import struct

from abc import ABCMeta, abstractmethod
from enum import Enum
from dataclasses import dataclass


RDMA_MAX_PAYLOAD = 1466


class IBuffer(metaclass=ABCMeta):
    @abstractmethod
    def pack(self) -> bytes:
        pass

    @classmethod
    @abstractmethod
    def unpack(cls, buf: bytes):
        pass

    @classmethod
    @abstractmethod
    def sizeof(cls) -> int:
        pass


class Command(Enum):
    UDPBD_CMD_INFO = 0x00
    UDPBD_CMD_INFO_REPLY = 0x01
    UDPBD_CMD_READ = 0x02
    UDPBD_CMD_READ_RDMA = 0x03
    UDPBD_CMD_WRITE = 0x04
    UDPBD_CMD_WRITE_RDMA = 0x05
    UDPBD_CMD_WRITE_DONE = 0x06


@dataclass
class Header(IBuffer):
    cmd: Command        # uint16_t (3 bits)
    cmdid: int          # uint16_t (5 bits)
    cmdpkt: int         # uint16_t (8 bits)

    def pack(self) -> bytes:
        cmd_mask = 0b0000000000011111
        cmdid_mask = 0b0000000011100000
        cmdpkt_mask = 0b1111111100000000
        cmd = self.cmd.value & cmd_mask
        cmdid = (self.cmdid << 5) & cmdid_mask
        cmdpkt = (self.cmdpkt << 8) & cmdpkt_mask
        v = cmd | cmdid | cmdpkt
        return struct.pack('<H', v)

    @classmethod
    def unpack(cls, buf: bytes):
        v, = struct.unpack('<H', buf[:2])
        cmd_mask = 0b0000000000011111
        cmdid_mask = 0b0000000011100000
        cmdpkt_mask = 0b1111111100000000
        cmd = v & cmd_mask
        cmdid = (v & cmdid_mask) >> 5
        cmdpkt = (v & cmdpkt_mask) >> 8
        return cls(cmd=Command(cmd), cmdid=cmdid, cmdpkt=cmdpkt)

    @classmethod
    def sizeof(cls) -> int:
        return 2


@dataclass
class InfoRequest(IBuffer):
    hdr: Header         # 2 bytes

    def pack(self) -> bytes:
        return self.hdr.pack()

    @classmethod
    def unpack(cls, buf: bytes):
        hdr = Header.unpack(buf)
        return cls(hdr=hdr)

    @classmethod
    def sizeof(cls) -> int:
        return 2


@dataclass
class InfoReply(IBuffer):
    hdr: Header         # 2 bytes
    sector_size: int    # uint32_t (4 bytes)
    sector_count: int   # uint32_t (4 bytes)

    def pack(self) -> bytes:
        packed_sector_size = struct.pack('<I', self.sector_size)
        packed_sector_count = struct.pack('<I', self.sector_count)
        return self.hdr.pack() + packed_sector_size + packed_sector_count

    @classmethod
    def unpack(cls, buf: bytes):
        hdr_buf, buf = buf[:2], buf[2:]
        sector_size_buf, buf = buf[:4], buf[4:]
        sector_count_buf, buf = buf[:4], buf[4:]

        hdr = Header.unpack(hdr_buf)
        sector_size, = struct.unpack('<I', sector_size_buf)
        sector_count, = struct.unpack('<I', sector_count_buf)
        return cls(hdr=hdr, sector_size=sector_size, sector_count=sector_count)

    @classmethod
    def sizeof(cls) -> int:
        return 10


@dataclass
class RWRequest(IBuffer):
    hdr: Header         # 2 bytes
    sector_nr: int      # uint32_t (4 bytes)
    sector_count: int   # uint16_t (2 bytes)

    def pack(self) -> bytes:
        packed_hdr = self.hdr.pack()
        packed_sector_nr = struct.pack('<I', self.sector_nr)
        packed_sector_count = struct.pack('<H', self.sector_count)
        return packed_hdr + packed_sector_nr + packed_sector_count

    @classmethod
    def unpack(cls, buf: bytes):
        hdr_buf, buf = buf[:2], buf[2:]
        sector_nr_buf, buf = buf[:4], buf[4:]
        sector_count_buf, buf = buf[:2], buf[2:]

        hdr = Header.unpack(hdr_buf)
        sector_nr, = struct.unpack('<I', sector_nr_buf)
        sector_count, = struct.unpack('<H', sector_count_buf)
        return cls(hdr=hdr, sector_nr=sector_nr, sector_count=sector_count)

    @classmethod
    def sizeof(cls) -> int:
        return 8


@dataclass
class WriteReply(IBuffer):
    hdr: Header  # 2 bytes
    result: int  # uint32_t (4 bytes)

    def pack(self) -> bytes:
        packed_hdr = self.hdr.pack()
        packed_result = struct.pack('<I', self.result)
        return packed_hdr + packed_result

    @classmethod
    def unpack(cls, buf: bytes):
        hdr_buf, buf = buf[:2], buf[2:]
        result_buf, buf = buf[:4], buf[4:]

        hdr = Header.unpack(hdr_buf)
        result, = struct.unpack('<I', result_buf)
        return cls(hdr=hdr, result=result)

    @classmethod
    def sizeof(cls) -> int:
        return 6


@dataclass
class BlockType(IBuffer):
    block_shift: int    # uint32_t (4 bits)
    block_count: int    # uint32_t (9 bits)
    spare: int          # uint32_t (19 bits)

    @classmethod
    def create(cls, block_count: int, block_size: int = 512):
        def calc_block_size_from_shift(s: int) -> int:
            return 1 << (s + 2)

        for shift in range(8):
            calculated_block_size = calc_block_size_from_shift(shift)
            if calculated_block_size == block_size:
                return cls(block_shift=shift, block_count=block_count, spare=0)
        raise RuntimeError('unsupported block size')

    def pack(self) -> bytes:
        packed_block_shift = self.block_shift & 0b00000000000000000000000000001111
        packed_block_count = (self.block_count << 4) & 0b00000000000000000000011111110000
        packed_spare = (self.spare << 13) & 0b11111111111111111110000000000000
        v = packed_block_shift | packed_block_count | packed_spare
        return struct.pack('<I', v)

    @classmethod
    def unpack(cls, buf: bytes):
        v, = struct.unpack('<I', buf[:4])
        block_shift = v & 0b00000000000000000000000000001111
        block_count = (v & 0b00000000000000000000011111110000) >> 4
        spare = (v & 0b11111111111111111110000000000000) >> 13
        return cls(block_shift=block_shift, block_count=block_count, spare=spare)

    @classmethod
    def sizeof(cls) -> int:
        return 4


@dataclass
class RDMAPayload(IBuffer):
    hdr: Header             # 2 bytes
    bt: BlockType           # 4 bytes
    data: bytes             # 1466 bytes

    def pack(self) -> bytes:
        packed_hdr = self.hdr.pack()
        packed_block_type = self.bt.pack()
        return packed_hdr + packed_block_type + self.data

    @classmethod
    def unpack(cls, buf: bytes):
        hdr_buf, buf = buf[:2], buf[2:]
        block_type_buf, buf = buf[:4], buf[4:]

        hdr = Header.unpack(hdr_buf)
        block_type = BlockType.unpack(block_type_buf)
        data = buf

        return cls(hdr=hdr, bt=block_type, data=data)

    @classmethod
    def sizeof(cls) -> int:
        return 1472
