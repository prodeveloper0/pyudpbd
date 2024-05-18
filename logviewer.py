import logging

from pyudpbd.sockutils import create_broadcast_socket


def main():
    logging.basicConfig(level=logging.DEBUG)

    sock = create_broadcast_socket(18194)

    while True:
        try:
            data, addr = sock.recvfrom(1024)
        except KeyboardInterrupt:
            break
        ipaddr, port = addr
        print(f'[{ipaddr}:{port}] {data.decode().strip()}')
    sock.close()


if __name__ == '__main__':
    main()
