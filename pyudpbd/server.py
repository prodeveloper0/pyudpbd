import logging
from contextlib import suppress

from typing import Any, Optional

from pyudpbd.proto import IBuffer, Header, Command, RDMA_MAX_PAYLOAD, BlockType
from pyudpbd.proto import InfoRequest, InfoReply
from pyudpbd.proto import RWRequest, WriteReply, RDMAPayload

from pyudpbd.blkdev import IBlockDevice

from pyudpbd.sockutils import create_broadcast_socket


logger = logging.getLogger('pyudpbd.server')


class BlockDeviceServer:
    # https://github.com/israpps/udpbd-server
    @staticmethod
    def _parse_buffer_by_header(header: Header, buf: bytes) -> IBuffer:
        match header.cmd:
            case Command.UDPBD_CMD_INFO:
                return InfoRequest.unpack(buf)
            case Command.UDPBD_CMD_READ:
                return RWRequest.unpack(buf)
            case Command.UDPBD_CMD_WRITE:
                return RWRequest.unpack(buf)
            case Command.UDPBD_CMD_WRITE_RDMA:
                return RDMAPayload.unpack(buf)
        raise ValueError(f'Unknown command: {header.cmd.name}')

    def __init__(self, blkdev: IBlockDevice, port: int = 0xbdbd):
        self._blkdev = blkdev
        self._port = port
        self._socket = create_broadcast_socket(port)

        self._write_size_left = 0
        self._block_shift = 0
        self._block_size = 0
        self._blocks_per_packet = 0
        self._blocks_per_sector = 0

        self._set_block_shift(5)

    def _set_block_shift(self, shift: int):
        prev_block_size = self._block_size
        if shift != self._block_shift:
            self._block_shift = shift
            self._block_size = 1 << (self._block_shift + 2)
            self._blocks_per_packet = RDMA_MAX_PAYLOAD // self._block_size
            self._blocks_per_sector = self._blkdev.sector_size // self._block_size
            logger.debug(f'change block size. {prev_block_size} -> {self._block_size}')

    def _set_block_shift_sectors(self, sectors: int):
        shift = 3
        size = sectors * self._blkdev.sector_size
        packetsMIN = (size + 1440 - 1) // 1440
        packets128 = (size + 1408 - 1) // 1408
        packets256 = (size + 1280 - 1) // 1280
        packets512 = (size + 1024 - 1) // 1024
        if packets512 == packetsMIN:
            shift = 7
        elif packets256 == packetsMIN:
            shift = 6
        elif packets128 == packetsMIN:
            shift = 5
        self._set_block_shift(shift)

    def _handle_info_request(self, req: InfoRequest, addr: Any):
        logger.info(f'info request. {addr[0]}:{addr[1]}, cmdid={req.hdr.cmdid}, cmdpkt={req.hdr.cmdpkt}')
        reply = InfoReply(
            hdr=Header(cmd=Command.UDPBD_CMD_INFO_REPLY, cmdid=req.hdr.cmdid, cmdpkt=1),
            sector_size=self._blkdev.sector_size,
            sector_count=self._blkdev.sector_count
        )
        self._socket.sendto(reply.pack(), addr)

    def _handle_read_request(self, req: RWRequest, addr: Any):
        reply = RDMAPayload(
            hdr=Header(cmd=Command.UDPBD_CMD_READ_RDMA, cmdid=req.hdr.cmdid, cmdpkt=1),
            bt=BlockType(
                block_shift=self._block_shift,
                block_count=0,
                spare=0
            ),
            data=b''
        )
        logger.info(
            f'RDMA read. '
            f'cmdid={req.hdr.cmdid}, '
            f'startSector={req.sector_nr}, '
            f'sectorCount={req.sector_count}.'
        )
        self._set_block_shift_sectors(req.sector_count)
        self._blkdev.seek(req.sector_nr)

        blocks_left = req.sector_count * self._blocks_per_sector
        while blocks_left > 0:
            reply.bt.block_count = self._blocks_per_packet if blocks_left > self._blocks_per_packet else blocks_left
            blocks_left -= reply.bt.block_count
            buf = self._blkdev.read(reply.bt.block_count * self._block_size)
            reply.data = buf
            self._socket.sendto(reply.pack(), addr)
            reply.hdr.cmdpkt += 1

    def _handle_write_request(self, req: RWRequest, addr: Any):
        self._blkdev.seek(req.sector_nr)
        self._write_size_left = req.sector_count * self._blkdev.sector_size
        logger.debug(f'write. writeSizeLeft={self._write_size_left}')

    def _handle_write_rdma(self, req: RDMAPayload, addr: Any):
        size = req.bt.block_count * (1 << (req.bt.block_shift + 2))
        self._blkdev.write(req.data)
        self._write_size_left -= size

        logger.info(
            f'RDMA write. '
            f'cmdid={req.hdr.cmdid}, '
            f'dataSize={len(req.data)}, '
            f'writeSizeLeft={self._write_size_left}, '
            f'done={self._write_size_left == 0}.'
        )

        if self._write_size_left == 0:
            reply = WriteReply(
                hdr=Header(cmd=Command.UDPBD_CMD_WRITE_DONE, cmdid=req.hdr.cmdid, cmdpkt=req.hdr.cmdid + 1),
                result=0
            )
            self._socket.sendto(reply.pack(), addr)

    def serve(self, timeout: Optional[float] = None):
        logger.info('server is started')
        self._socket.settimeout(timeout)
        while True:
            if not self._blkdev.available():
                logger.error('block device is not available')
                break

            try:
                buf, addr = self._socket.recvfrom(2048)
            except TimeoutError:
                continue
            except OSError as e:
                logger.error(e)
                break

            header = Header.unpack(buf[:2])
            logger.debug(f'{header.cmd} - {addr[0]}:{addr[1]}')

            payload = self._parse_buffer_by_header(header, buf)
            match header.cmd:
                case Command.UDPBD_CMD_INFO:
                    req: InfoRequest = payload
                    self._handle_info_request(req, addr)
                case Command.UDPBD_CMD_READ:
                    req: RWRequest = payload
                    self._handle_read_request(req, addr)
                case Command.UDPBD_CMD_WRITE:
                    req: RWRequest = payload
                    self._handle_write_request(req, addr)
                case Command.UDPBD_CMD_WRITE_RDMA:
                    req: RDMAPayload = payload
                    self._handle_write_rdma(req, addr)
        logger.info('server is stopped')

    def close(self):
        with suppress(OSError):
            self._socket.close()
