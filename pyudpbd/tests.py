from socket import socket
from socket import AF_INET, SOCK_DGRAM
from socket import SOL_SOCKET, SO_BROADCAST

import pytest

from pyudpbd.proto import InfoReply, InfoRequest, Header, Command, RWRequest, RDMAPayload, BlockType, WriteReply


@pytest.fixture(scope='function')
def sock():
    sock = socket(AF_INET, SOCK_DGRAM)
    sock.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
    yield sock
    sock.close()


def test_info_request(sock: socket):
    request = InfoRequest(
        hdr=Header(cmd=Command.UDPBD_CMD_INFO, cmdid=1, cmdpkt=0)
    )
    sock.sendto(request.pack(), ('255.255.255.255', 48573))
    buf, addr = sock.recvfrom(2048)
    reply = InfoReply.unpack(buf)
    pass


def test_read_request(sock: socket):
    request = RWRequest(
        hdr=Header(cmd=Command.UDPBD_CMD_READ, cmdid=1, cmdpkt=0),
        sector_nr=1,
        sector_count=2
    )
    sock.sendto(request.pack(), ('255.255.255.255', 48573))
    buf, addr = sock.recvfrom(2048)
    reply = RDMAPayload.unpack(buf)
    pass


def test_write_request(sock: socket):
    request = RWRequest(
        hdr=Header(cmd=Command.UDPBD_CMD_WRITE, cmdid=1, cmdpkt=0),
        sector_nr=1,
        sector_count=2
    )
    sock.sendto(request.pack(), ('255.255.255.255', 48573))

    request = RDMAPayload(
        hdr=Header(cmd=Command.UDPBD_CMD_WRITE_RDMA, cmdid=1, cmdpkt=0),
        bt=BlockType.create(block_count=2, block_size=512),
        data=b''.join([f'{i}'.encode() * 512 for i in range(2)])
    )
    sock.sendto(request.pack(), ('255.255.255.255', 48573))

    buf, addr = sock.recvfrom(2048)
    reply = WriteReply.unpack(buf)
    pass


def test_send_debug_log(sock: socket):
    sock.bind(('', 0))
    sock.sendto(b'Test message', ('255.255.255.255', 18194))
