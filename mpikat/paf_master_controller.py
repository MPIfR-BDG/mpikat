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
import coloredlogs
import json
import tornado
import signal
import time
import numpy as np
from optparse import OptionParser
from tornado.gen import Return, coroutine
from tornado.iostream import IOStream
from katcp import Sensor, AsyncReply
from katcp.kattypes import request, return_reply, Int, Str, Discrete, Float
from mpikat.master_controller import MasterController
from mpikat.paf_product_controller import PafProductController
from mpikat.paf_worker_wrapper import PafWorkerPool
from mpikat.exceptions import ProductLookupError

# ?halt message means shutdown everything and power off all machines

log = logging.getLogger("mpikat.paf_master_controller")

PAF_PRODUCT_ID = "pafbackend"

class ScpiInterface(object):
    def __init__(self, port):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setsocketopts(socket.SO_REUSEADDR)
        self._address = ("", port)
        self._iostream = IOStream(self._socket)


class PafMasterController(MasterController):
    """This is the main KATCP interface for the PAF
    pulsar searching system on MeerKAT. This controller only
    holds responsibility for capture of data from the CBF
    network and writing of that data to disk.

    This interface satisfies the following ICDs:
    CAM-PAF: <link>
    """
    VERSION_INFO = ("mpikat-paf-api", 0, 1)
    BUILD_INFO = ("mpikat-paf-implementation", 0, 1, "rc1")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]

    CONTROL_MODES = ["KATCP", "SCPI"]
    KATCP, SCPI = CONTROL_MODES

    def __init__(self, ip, port):
        """
        @brief       Construct new PafMasterController instance

        @params  ip       The IP address on which the server should listen
        @params  port     The port that the server should bind to
        """
        self._control_mode = self.KATCP
        super(PafMasterController, self).__init__(ip, port, PafWorkerPool())

    @property
    def katcp_control_mode(self):
        return self._control_mode == self.KATCP
    
    @property
    def scpi_control_mode(self):
        return self._control_mode == self.SCPI

    @request(Str())
    @return_reply()
    def request_set_control_mode(self, req, mode):
        """ Weeeee """
        mode = mode.upper()
        if not mode in self.CONTROL_MODES:
            return ("fail", "Unknown mode '{}', valid modes are '{}' ".format(
                mode, ", ".join(self.CONTROL_MODES)))
        else:
            self._control_mode = mode
        return ("ok",)

    @request(Str())
    @return_reply()
    def request_configure(self, req, dummy_config_string):
        """
        @brief      Configure PAF to receive and process data

        @param      req               A katcp request object

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """
        if not self.katcp_control_mode:
            return ("fail", "Master controller is in control mode: {}".format(self._control_mode))
        @coroutine
        def configure_wrapper():
            response = yield self.configure(dummy_config_string)
            req.reply(*response)
        self.ioloop.add_callback(configure_wrapper)
        raise AsyncReply

    @coroutine
    def configure(self, dummy_config_string):
        log.info("Configuring PAF processing")
        if self._products:
            raise Return(("fail", "PAF already has a configured data product"))
        self._products[PAF_PRODUCT_ID] = PafProductController(self, PAF_PRODUCT_ID)
        self._update_products_sensor()
        try:
            yield self._products[PAF_PRODUCT_ID].configure(dummy_config_string)
        except Exception as error:
            log.error("Failed to configure product with error: {}".format(str(error)))
            raise Return(("ok", str(error)))
        else:
            log.debug("Configured PAF instance with ID: {}".format(PAF_PRODUCT_ID))
            raise Return(("ok",))

    @request()
    @return_reply()
    def request_deconfigure(self, req):
        """
        @brief      Deconfigure the PAF instance.

        @note       Deconfigure the PAF instance. If PAF uses katportalclient to get information
                    from CAM, then it should disconnect at this time.

        @param      req               A katcp request object

        @param      PAF_PRODUCT_ID        This is a name for the data product, used to track which subarray is being deconfigured.
                                      For example "array_1_bc856M4k".

        @return     katcp reply object [[[ !deconfigure ok | (fail [error description]) ]]]
        """
        if not self.katcp_control_mode:
            return ("fail", "Master controller is in control mode: {}".format(self._control_mode))
        @coroutine
        def deconfigure_wrapper():
            response = yield self.deconfigure()
            req.reply(*response)
        self.ioloop.add_callback(deconfigure_wrapper)
        raise AsyncReply

    @coroutine
    def deconfigure(self):
        log.info("Deconfiguring PAF processing")
        try:
            product = self._get_product(PAF_PRODUCT_ID)
        except ProductLookupError as error:
            raise Return(("fail", str(error)))
        try:
            yield product.deconfigure()
        except Exception as error:
            raise Return(("fail", str(error)))
        del self._products[PAF_PRODUCT_ID]
        self._update_products_sensor()
        raise Return(("ok",))

    @request()
    @return_reply()
    def request_start_capture(self, req):
        """ arse """
        if not self.katcp_control_mode:
            return ("fail", "Master controller is in control mode: {}".format(self._control_mode))
        @coroutine
        def start_wrapper():
            response = yield self.start_capture()
            req.reply(*response)
        self.ioloop.add_callback(start_wrapper)
        raise AsyncReply

    @coroutine
    def start_capture(self):
        try:
            product = self._get_product(PAF_PRODUCT_ID)
        except ProductLookupError as error:
            raise Return(("fail", str(error)))
        try:
            yield product.start_capture()
        except Exception as error:
            raise Return(("fail", str(error)))
        else:
            raise Return(("ok",))

    @request()
    @return_reply()
    def request_stop_capture(self, req):
        """
        @brief      Stop PAF streaming

        @param      PAF_PRODUCT_ID      This is a name for the data product, used to track which subarray is being deconfigured.
                                    For example "array_1_bc856M4k".
        """
        if not self.katcp_control_mode:
            return ("fail", "Master controller is in control mode: {}".format(self._control_mode))
        @coroutine
        def stop_wrapper():
            response = yield self.stop_capture()
            req.reply(*response)
        self.ioloop.add_callback(stop_wrapper)
        raise AsyncReply

    @coroutine
    def stop_capture(self):
        try:
            product = self._get_product(PAF_PRODUCT_ID)
        except ProductLookupError as error:
            raise Return(("fail", str(error)))
        yield product.stop_capture()
        raise Return(("ok",))


@coroutine
def on_shutdown(ioloop, server):
    log.info("Shutting down server")
    yield server.stop()
    ioloop.stop()

def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--host', dest='host', type=str,
        help='Host interface to bind to')
    parser.add_option('-p', '--port', dest='port', type=long,
        help='Port number to bind to')
    parser.add_option('', '--log_level',dest='log_level',type=str,
        help='Port number of status server instance',default="INFO")
    (opts, args) = parser.parse_args()
    logger = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level=opts.log_level.upper(),
        logger=logger)
    logging.getLogger('katcp').setLevel('INFO')
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting PafMasterController instance")
    server = PafMasterController(opts.host, opts.port)
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, server))
    def start_and_display():
        server.start()
        log.info("Listening at {0}, Ctrl-C to terminate server".format(server.bind_address))

    ioloop.add_callback(start_and_display)
    ioloop.start()

if __name__ == "__main__":
    main()

