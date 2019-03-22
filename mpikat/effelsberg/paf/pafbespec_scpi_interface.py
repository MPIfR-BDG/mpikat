import json
import logging
from tornado.gen import coroutine
from mpikat.core.scpi import ScpiAsyncDeviceServer, scpi_request, raise_or_ok

log = logging.getLogger('mpikat.paf_scpi_interface')


class PafbeSpecScpiInterface(ScpiAsyncDeviceServer):

    def __init__(self, master_controller, interface, port, ioloop=None):
        """
        @brief      A SCPI interface for a PafMasterController instance

        @param      master_controller   A PafMasterController instance
        @param      interface    The interface to listen on for SCPI commands
        @param      interface    The port to listen on for SCPI commands
        @param      ioloop       The ioloop to use for async functions

        @note If no IOLoop instance is specified the current instance is used.
        """
        super(PafbeSpecScpiInterface, self).__init__(interface, port, ioloop)
        self._mc = master_controller
        self._config = {}

    @scpi_request()
    def request_pafbespec_configure(self, req):
        """
        @brief      Configure the PAF backend

        @param      req    An ScpiRequest object
        """
        active_sections = self._config["active_sections"]
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
                                                               self._mc.configure, active_sections, 1))

    @scpi_request(*([int, ] * 144))
    def request_pafbespec_cmdsections(self, req, *used_sections):
        """
        @brief      Set the used sections for the FI

        @param      req    An ScpiRequest object
        """
        self._config["active_sections"] = used_sections

    @scpi_request()
    def request_pafbespec_start(self, req):
        """
        @brief      Start data processing in the PAF backend

        @param      req    An ScpiRequest object
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
                                                               self._mc.capture_start))

    @scpi_request()
    def request_pafbespec_stop(self, req):
        """
        @brief      Stop data processing in the PAF backend

        @param      req    An ScpiRequest object
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
                                                               self._mc.capture_stop))

    @scpi_request()
    def request_pafbespec_abort(self, req):
        """
        @brief      Stop data processing in the PAF backend

        @param      req    An ScpiRequest object

        @note       This is a synonym for stop
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
                                                               self._mc.capture_stop))
