import socket
import logging

log = logging.getLogger('mpikat.utils.unix_socket')


class UDSClient(object):
    def __init__(self, socket_name):
        self._socket_name = socket_name
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            self._sock.connect(self._socket_name)
        except Exception:
            log.exception("Unable to connect to Unix domain socket {}".format(
                self._socket_name))
        self._sock.settimeout(2)

    def close(self):
        self._sock.close()

    def send(self, message):
        message += "\r\n"
        self._sock.sendall(message)

    def recv(self, maxsize=8192, timeout=2):
        self._sock.settimeout(2)
        return self._sock.recv(maxsize)
