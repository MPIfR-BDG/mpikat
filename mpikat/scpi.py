import socket
import errno
import logging
import coloredlogs
import signal
from tornado.ioloop import IOLoop
from tornado.gen import Return, coroutine, sleep
from threading import Thread, Event
from Queue import Queue
from datetime import datetime

log = logging.getLogger("mpikat.scpi")

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
        return self._data

    @property
    def command(self):
        return self._data.split()[0]

    @property
    def args(self):
        return self._data.split()[1:]

    def acknowledge(self):
        """
        @brief Return an SCPI acknowledgement to the original sender

        @detail The response consists of the original message followed by and ISO timestamp.
        """
        isotime = "{}UTC".format(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
        response = "{} {}".format(self.data, isotime)
        log.info("Acknowledging request '{}' from {}".format(self.data, self._addr))
        log.debug("Acknowledgement: {}".format(response))
        self._socket.sendto(response, self._addr)


class ScpiInterface(object):
    def __init__(self, interface, port, ioloop):
        """
        @brief      Class for scpi interface.

        @param interface    The interface on which to listen
        @param port         The port on which to listen
        @param ioloop       The ioloop in which to run the interface
        """
        log.debug("Creating SCPI interface on port {}".format(port))
        self._address = (interface, port)
        self._buffer_size = 4096
        self._ioloop = ioloop
        self._handlers = {}
        self._stop_event = Event()
        self._socket = None

    def __del__(self):
        self.stop()

    def reset_socket(self):
        """
        @brief      Reset the socket used by this interface
        """
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(self._address)
        self._socket.setblocking(False)

    def add_handler(self, command, callback):
        """
        @brief      Add an SCPI command handler
        
        @param      command   The specific SCPI command on which this handler should be called
        @param      callback  The callback to execute on receipt of the specified command
        
        @detail     The call signature of the call back should be of the form:
                    @code
                        def handler(command, *args)
                    @endcode
                    Where args is a list of strings that were received as arguments to the
                    SCPI command. It is the responsibility of the handler to parse these
                    arguments appropriately.
        """
        self._handlers[command] = callback

    def remove_handler(self, command):
        """
        @brief      Remove an SCPI command handler

        @param      command   The specific SCPI command for which the handler should be removed
        """
        if command in self._handlers:
            del self._handlers[command]

    def flush(self):
        log.debug("Flushing socket")
        while True:
            try:
                message, addr = self._socket.recvfrom(self._buffer_size)
                log.debug("Flushing message '{}' from {}:{}".format(message, addr[0], addr[1]))
            except:
                break

    def start(self):
        """
        @brief      Start the SCPI interface listening for new commands
        """
        log.info("Starting SCPI interface")
        self.reset_socket()
        self._stop_event.clear()
        self.flush()
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
                    self._ioloop.add_callback(self._dispatch, request)
        self._ioloop.add_callback(receiver)

    def stop(self):
        """
        @brief      Stop the SCPI interface listening for new commands
        """
        log.debug("Stopping SCPI interface")
        self._stop_event.set()
        if self._socket:
            self._socket.close()

    @coroutine
    def _dispatch(self, request):
        callback = self._handlers.get(request.command, self.default_handler)
        try:
            callback(request.command, *request.args)
        except Exception as error:
            log.error("Error while executing SCPI request callback: {}".format(str(error)))
        else:
            request.acknowledge()

    def default_handler(self, command, *args):
        """
        @brief      The default handler used by the interface. 
        """
        log.warning("Unhandled command '{}' with arguments '{}'".format(command, args))


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
    server = ScpiInterface("", 5000, ioloop)
    def handle(command, frequency):
        print "Setting frequency to {}".format(frequency)
    server.add_handler("scpi:eddbackend:configure:frequency", 
        lambda command, frequency: ioloop.add_callback(handle, command, frequency))
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, server))
    ioloop.add_callback(server.start)
    ioloop.start()
