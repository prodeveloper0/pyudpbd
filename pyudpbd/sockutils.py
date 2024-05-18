from socket import socket
from socket import AF_INET, SOCK_DGRAM
from socket import SOL_SOCKET, SO_BROADCAST


def create_broadcast_socket(port: int, host: str = '0.0.0.0') -> socket:
    s = socket(AF_INET, SOCK_DGRAM)
    s.bind((host, port))
    s.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
    return s
