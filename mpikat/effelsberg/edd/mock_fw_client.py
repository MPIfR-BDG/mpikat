"""
Copyright (c) 2018 Ewan Barr <ebarr@mpifr-bonn.mpg.de>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import logging
import socket
import errno
import ctypes as C
import numpy as np
import coloredlogs
import signal
import os
from tornado.gen import coroutine, sleep, Return, TimeoutError
from tornado.locks import Event, Condition
from tornado.ioloop import IOLoop
from argparse import ArgumentParser

log = logging.getLogger("mpikat.mock_fw_client")

TYPE_MAP = {
        "F": (C.c_float, "float32"),
        "I": (C.c_uint32, "uint32")
    }


class StopEvent(Exception):
    pass


class MockFitsWriterClientError(Exception):
    pass


class FWSectionHeader(C.LittleEndianStructure):
    _fields_ = [
        ('section_id', C.c_uint32),
        ('nchannels', C.c_uint32)
    ]

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__, ", ".join(
            ["{} = {}".format(
                key, getattr(self, key)) for key, _ in self._fields_]))


class FWHeader(C.LittleEndianStructure):
    _fields_ = [
        ("data_type", C.c_char * 4),
        ("channel_data_type", C.c_char * 4),
        ("packet_size", C.c_uint32),
        ("backend_name", C.c_char * 8),
        ("timestamp", C.c_char * 28),
        ("integration_time", C.c_uint32),
        ("blank_phases", C.c_uint32),
        ("nsections", C.c_uint32),
        ("blocking_factor", C.c_uint32)
    ]

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__, ", ".join(
            ["{} = {}".format(
                key, getattr(self, key)) for key, _ in self._fields_]))


class MockFitsWriterClient(object):
    """
    Wrapper class for a KATCP client to a EddFitsWriterServer
    """
    def __init__(self, address, record_dest):
        """
        @brief      Construct new instance
                    If record_dest is not empty, create a folder named record_dest and record the received packages there.
        """
        self._address = address
        self.__record_dest = record_dest
        if record_dest:
            if not os.path.isdir(record_dest):
                os.makedirs(record_dest)
        self._ioloop = IOLoop.current()
        self._stop_event = Event()
        self._is_stopped = Condition()
        self._socket = None
        self.__last_package = 0

    def reset_connection(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setblocking(False)
        try:
            self._socket.connect(self._address)
        except socket.error as error:
            if error.args[0] == errno.EINPROGRESS:
                pass
            else:
                raise error

    @coroutine
    def recv_nbytes(self, nbytes):
        received_bytes = 0
        data = b''
        while received_bytes < nbytes:
            if self._stop_event.is_set():
                raise StopEvent
            try:
                log.debug("Requesting {} bytes".format(nbytes - received_bytes))
                current_data = self._socket.recv(nbytes - received_bytes)
                received_bytes += len(current_data)
                data += current_data
                log.debug("Received {} bytes ({} of {} bytes)".format(
                    len(current_data), received_bytes, nbytes))
            except socket.error as error:
                error_id = error.args[0]
                if error_id == errno.EAGAIN or error_id == errno.EWOULDBLOCK:
                    yield sleep(0.1)
                else:
                    log.exception(
                        "Unexpected error on socket recv: {}".format(
                            str(error)))
                    raise error
        raise Return(data)

    @coroutine
    def recv_loop(self):
        try:
            header, sections = yield self.recv_packet()
        except StopEvent:
            log.debug("Notifying that recv calls have stopped")
            self._is_stopped.notify()
        except Exception:
            log.exception("Failure while receiving packet")
        else:
            self._ioloop.add_callback(self.recv_loop)

    def start(self):
        self._stop_event.clear()
        self.reset_connection()
        self._ioloop.add_callback(self.recv_loop)

    @coroutine
    def stop(self, timeout=2):
        self._stop_event.set()
        try:
            success = yield self._is_stopped.wait(
                timeout=self._ioloop.time() + timeout)
            if not success:
                raise TimeoutError
        except TimeoutError:
            log.error(("Could not stop the client within "
                       "the {} second limit").format(timeout))
        except Exception:
            log.exception("Fucup")

    @coroutine
    def recv_packet(self):
        log.debug("Receiving packet header")
        raw_header = yield self.recv_nbytes(C.sizeof(FWHeader))
        log.debug("Converting packet header")
        header = FWHeader.from_buffer_copy(raw_header)
        log.info("Received header: {}".format(header))
        if header.timestamp < self.__last_package:
            log.error("Timestamps out of order!")
        else:
            self.__last_package = header.timestamp

        if self.__record_dest:
            filename = os.path.join(self.__record_dest, "FWP_{}.dat".format(header.timestamp))
            while os.path.isfile(filename):
                log.warning('Filename {} already exists. Add suffix _'.format(filename))
                filename += '_'
            log.info('Recording to file {}'.format(filename))
            ofile = open(filename, 'wb')
            ofile.write(raw_header)

        fw_data_type = header.channel_data_type.strip().upper()
        c_data_type, np_data_type = TYPE_MAP[fw_data_type]
        sections = []
        for section in range(header.nsections):
            log.debug("Receiving section {} of {}".format(
                section+1, header.nsections))
            raw_section_header = yield self.recv_nbytes(C.sizeof(FWSectionHeader))
            if self.__record_dest:
                ofile.write(raw_section_header)

            section_header = FWSectionHeader.from_buffer_copy(raw_section_header)
            log.info("Section {} header: {}".format(section, section_header))
            log.debug("Receiving section data")
            raw_bytes = yield self.recv_nbytes(C.sizeof(c_data_type)
                                               * section_header.nchannels)
            if self.__record_dest:
                ofile.write(raw_bytes)
            data = np.frombuffer(raw_bytes, dtype=np_data_type)
            log.info("Section {} data: {}".format(section, data[:10]))
            sections.append((section_header, data))

        if self.__record_dest:
            ofile.close()
        raise Return((header, sections))


@coroutine
def on_shutdown(ioloop, client):
    log.info("Shutting down client")
    yield client.stop()
    ioloop.stop()


if __name__ == "__main__":
    parser = ArgumentParser(description="Receive (and optionally record) tcp packages send by EDD Fits writer interface.")
    parser.add_argument('-H', '--host', dest='host', type=str,
                      help='Host interface to connect to')
    parser.add_argument ('-p', '--port', dest='port', type=int,
                      help='Port number to connect to')
    parser.add_argument('--log-level', dest='log_level', type=str, help='Log level', default="INFO")

    parser.add_argument('--record-to', dest='record_to', type=str,
                      help='Destination to record data to. No recording if empty', default="")
    args = parser.parse_args()
    logging.getLogger().addHandler(logging.NullHandler())
    logger = logging.getLogger('mpikat')
    logging.getLogger('katcp').setLevel(logging.DEBUG)
    coloredlogs.install(
        fmt=("[ %(levelname)s - %(asctime)s - %(name)s "
             "- %(filename)s:%(lineno)s] %(message)s"),
        level=args.log_level.upper(),
        logger=logger)
    ioloop = IOLoop.current()
    log.info("Starting MockFitsWriterClient instance")
    client = MockFitsWriterClient((args.host, args.port), args.record_to)
    signal.signal(
        signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
            on_shutdown, ioloop, client))
    client.start()
    ioloop.start()
