import socket
import errno
import logging
import coloredlogs
import signal
import shlex
from tornado.ioloop import IOLoop
from tornado.gen import Return, coroutine, sleep
from threading import Thread, Event
from Queue import Queue
from datetime import datetime

log = logging.getLogger("mpikat.scpi")

class UnhandledScpiRequest(Exception):
    pass

class MalformedScpiRequest(Exception):
    pass

class ScpiRequest(object):
    def __init__(self, data, addr, socket):
        """
        @brief      Class for wrapping scpi requests.

        @param data   The data in the SCPI message
        @param addr   The address of the message origin
        @param socket The socket instance on which this message was received
        """
        self._data = data
        self._addr = addr
        self._socket = socket

    @property
    def data(self):
        return self._data.rstrip()

    @property
    def command(self):
        return self._data.split()[0].lower()

    @property
    def args(self):
        return shlex.split(self._data)[1:]

    def _send_response(self, msg):
        isotime = "{}UTC".format(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
        response = "{} {}".format(msg, isotime)
        log.info("Acknowledging request '{}' from {}".format(self.data, self._addr))
        log.debug("Acknowledgement: {}".format(response))
        self._socket.sendto(response, self._addr)

    def error(self, error_msg):
        """
        @brief Return an SCPI error to the original sender

        @detail The response consists of the original message followed by and ISO timestamp.
        """
        self._send_response("{} ERROR {}".format(self.data, error_msg))

    def ok(self):
        """
        @brief Return an SCPI acknowledgement to the original sender

        @detail The response consists of the original message followed by and ISO timestamp.
        """
        self._send_response(self.data)


class ScpiAsyncDeviceServer(object):
    def __init__(self, interface, port, ioloop=None):
        """
        @brief      Class for scpi interface.

        @param interface    The interface on which to listen
        @param port         The port on which to listen
        @param ioloop       The ioloop in which to run the interface
        """
        log.debug("Creating SCPI interface on port {}".format(port))
        self._address = (interface, port)
        self._buffer_size = 4096
        self._ioloop = IOLoop.current() if not ioloop else ioloop
        self._stop_event = Event()
        self._socket = None

    def __del__(self):
        self.stop()

    def _create_socket(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(self._address)
        self._socket.setblocking(False)

    def _flush(self):
        log.debug("Flushing socket")
        while True:
            try:
                message, addr = self._socket.recvfrom(self._buffer_size)
                log.debug("Flushing message '{}' from {}:{}".format(message, addr[0], addr[1]))
            except:
                break

    def _make_coroutine_wrapper(self, req, cr, *args, **kwargs):
        @coroutine
        def wrapper():
            try:
                yield cr(*args, **kwargs)
            except Exception as error:
                log.error(str(error))
                req.error(str(error))
            else:
                req.ok()
        return wrapper

    def start(self):
        """
        @brief      Start the SCPI interface listening for new commands
        """
        log.info("Starting SCPI interface")
        self._create_socket()
        self._stop_event.clear()
        self._flush()

        @coroutine
        def receiver():
            request = None
            try:
                message, addr = self._socket.recvfrom(self._buffer_size)
                log.info("Message received from {}: {}".format(addr, message))
            except socket.error as error:
                error_id = error.args[0]
                if error_id == errno.EAGAIN or error_id == errno.EWOULDBLOCK:
                    yield sleep(0.2)
                else:
                    raise error
            except Exception as error:
                log.exception("Error while fetching SCPI message")
                raise error
            else:
                request = ScpiRequest(message, addr, self._socket)
            finally:
                if not self._stop_event.is_set():
                    self._ioloop.add_callback(receiver)
                if request:
                    yield self._dispatch(request)
        self._ioloop.add_callback(receiver)

    def stop(self):
        """
        @brief      Stop the SCPI interface listening for new commands
        """
        log.debug("Stopping SCPI interface")
        self._stop_event.set()
        if self._socket:
            self._socket.close()
            self._socket = None

    @coroutine
    def _dispatch(self, req):
        handler_name = "request_{}".format(req.command.replace(":","_"))
        log.debug("Searching for handler '{}'".format(handler_name))
        try:
            handler = self.__getattribute__(handler_name)
        except AttributeError:
            log.error("No handler named '{}'".format(handler_name))
            req.error("UNKNOWN COMMAND")
            raise UnhandledScpiRequest(req.command)
        except Exception as error:
            log.error("Exception during handler lookup: {}".format(str(error)))
            req.error(str(error))
            raise error
        else:
            log.debug('Executing handler')
            handler(req)


def scpi_request(*types):
    def wrapper(func):
        def request_handler(obj, req):
            if len(types) != len(req.args):
                message = "Expected {} arguments but received {}".format(
                    len(types), len(req.args))
                log.error(message)
                req.error("INCORRECT NUMBER OF ARGUMENTS")
                raise MalformedScpiRequest(message)

            new_args = []
            for ii, (arg, type_) in enumerate(zip(req.args, types)):
                try:
                    new_args.append(type_(arg))
                except Exception as error:
                    log.error(str(error))
                    req.error("UNABLE TO PARSE ARGUMENTS")
                    raise MalformedScpiRequest(str(error))
            func(obj, req, *new_args)
        return request_handler
    return wrapper

def raise_or_ok(func):
    def wrapper(obj, req, *args, **kwargs):
        try:
            func(obj, req, *args, **kwargs)
        except Exception as error:
            req.error(str(error))
        else:
            req.ok()
    return wrapper


class Example(ScpiAsyncDeviceServer):
    def __init__(self, interface, port, ioloop=None):
        super(Example, self).__init__(interface, port, ioloop)
        self._config = {}

    @scpi_request(str)
    @raise_or_ok
    @coroutine
    def request_dummy_test(self, req, message):
        print message

@coroutine
def on_shutdown(ioloop, server):
    log.info("Shutting down interface")
    yield server.stop()
    ioloop.stop()

if __name__ == "__main__":
    # This line is required to bypass tornado error when no handler is
    # is set on the root logger
    logging.getLogger().addHandler(logging.NullHandler())
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level=logging.DEBUG,
        logger=log)
    ioloop = IOLoop.current()
    server = Example("", 5000, ioloop)
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, server))
    ioloop.add_callback(server.start)
    ioloop.start()
