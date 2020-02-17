import threading
import time
import random
import tornado
import logging
import signal
import json
from katcp import AsyncDeviceServer, Message, DeviceServer, Sensor, ProtocolFlags, AsyncReply
from katcp.kattypes import (Str, Float, Timestamp,
                            Discrete, request, return_reply)
from katcp.resource_client import KATCPClientResource

import sys

log = logging.getLogger("mpikat.effelsberg.edd.pipeline.pipeline")


class PngKatcpServer(AsyncDeviceServer):

    VERSION_INFO = ("mpikat-api", 1, 0)
    BUILD_INFO = ("mpikat-edd-implementation", 0, 1, "rc1")

    PROTOCOL_INFO = ProtocolFlags(5, 0, set([
        ProtocolFlags.MULTI_CLIENT,
        ProtocolFlags.MESSAGE_IDS,
    ]))

    def setup_sensors(self):
        """Setup some server sensors."""
        self._tscrunch = Sensor.string(
            "tscrunch_PNG",
            description="tscrunch png",
            default=BLANK_IMAGE,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._tscrunch)

        self._fscrunch = Sensor.string(
            "fscrunch_PNG",
            description="fscrunch png",
            default=BLANK_IMAGE,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._fscrunch)

        self._profile = Sensor.string(
            "profile_PNG",
            description="pulse profile png",
            default=BLANK_IMAGE,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._profile)

    @request(Str())
    @return_reply()
    def request_profile(self, req, png_blob):
        self._profile.set_value(png_blob)
        req.reply("ok",)
        raise AsyncReply

    @request(Str())
    @return_reply()
    def request_fscrunch(self, req, png_blob):
        self._fscrunch.set_value(png_blob)
        req.reply("ok",)
        raise AsyncReply

    @request(Str())
    @return_reply()
    def request_tscrunch(self, req, png_blob):
        self._tscrunch.set_value(png_blob)
        req.reply("ok",)
        raise AsyncReply


@tornado.gen.coroutine
def on_shutdown(ioloop, server):
    print('Shutting down')
    yield server.stop()
    ioloop.stop()


def main()
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--host', dest='host', type=str,
                      help='Host interface to bind to', default="0.0.0.0")
    parser.add_option('-p', '--port', dest='port', type=long,
                      help='Port number to bind to', default=9000)
    parser.add_option('', '--log_level', dest='log_level', type=str,
                      help='logging level', default="INFO")
    (opts, args) = parser.parse_args()
    logging.getLogger().addHandler(logging.NullHandler())
    logger = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level=opts.log_level.upper(),
        logger=logger)
    logging.getLogger('katcp').setLevel('INFO')
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting PngKatcpServer instance")
    server = PngKatcpServer(opts.host, opts.port)
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, server))

    def start_and_display():
        server.start()
        log.info(
            "Listening at {0}, Ctrl-C to terminate server".format(server.bind_address))

    ioloop.add_callback(start_and_display)
    ioloop.start()

if __name__ == "__main__":
    main()