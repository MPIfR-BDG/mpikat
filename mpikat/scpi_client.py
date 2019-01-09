import socket
import sys
import logging
import coloredlogs
import errno
import time
from tornado.gen import coroutine, Return, sleep
from optparse import OptionParser

logger = logging.getLogger('mpikat.scpi_client')

class ScpiFailedRequest(Exception):
    pass

class ScpiClient(object):
    def __init__(self, ip, port, timeout=2):
        self._ip = ip
        self._port = port

    def send(self, message, timeout=2):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(timeout)
        logger.info("Sending '{}' to {}:{}".format(message, self._ip, self._port))
        sock.sendto(message, (self._ip, self._port))
        try:
            message,addr = sock.recvfrom(4096)
        except socket.timeout as error:
            logger.error("Timeout while waiting for SCPI acknowledgement")
            raise error
        else:
            logger.info("Received response '{}' from {}:{}".format(message, addr[0], addr[1]))
            if "ERROR" in message:
                raise ScpiFailedRequest(message)
        finally:
            sock.close()


class AsyncScpiClient(object):
    def __init__(self, ip, port, ioloop, timeout=2):
        self._ip = ip
        self._port = port
        self._ioloop = ioloop

    @coroutine
    def send(self, message, timeout=2):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(False)
        logger.info("Sending '{}' to {}:{}".format(message, self._ip, self._port))
        sock.sendto(message, (self._ip, self._port))
        start = time.time()
        try:
            while time.time() - start < timeout:
                try:
                    message, addr = sock.recvfrom(4096)
                    logger.info("Message received from {}: {}".format(addr, message))
                    break
                except socket.error as error:
                    error_id = error.args[0]
                    if error_id == errno.EAGAIN or error_id == errno.EWOULDBLOCK:
                        yield sleep(0.2)
                    else:
                        raise error
                except Exception as error:
                    logger.exception("Error while fetching SCPI message: {}".format(str(error)))
                    raise error
            else:
                raise socket.timeout
        except Exception as error:
            raise error
        else:
            if "ERROR" in message:
                raise ScpiFailedRequest(message)
        finally:
            sock.close()


if __name__ == "__main__":
    usage = "usage: %prog [options]"
    parser = OptionParser(usage = usage)
    parser.add_option('-H', '--host', dest='host', type=str, help='IP to send to')
    parser.add_option('-p', '--port', dest='port', type=long, help='Port number to send to')
    parser.add_option('-m', '--msg', dest='msg',type=str, help='Message to send')
    parser.add_option('', '--log-level',dest='log_level',type=str, help='Set the logging level',default="INFO")
    (opts, args) = parser.parse_args()
    logger = logging.getLogger('mpikat.scpi_client')
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level=opts.log_level.upper(),
        logger=logger)
    logger.setLevel(opts.log_level.upper())
    client = ScpiClient(opts.host, opts.port)
    client.send(opts.msg)
