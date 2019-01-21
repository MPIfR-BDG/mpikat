import json
import logging
from tornado.gen import coroutine
from mpikat.core.scpi import ScpiAsyncDeviceServer, scpi_request, raise_or_ok

log = logging.getLogger('mpikat.edd_scpi_interface')

class EddScpiInterface(ScpiAsyncDeviceServer):
    def __init__(self, master_controller, interface, port, ioloop=None):
        """
        @brief      A SCPI interface for a EddMasterController instance

        @param      master_controller   A EddMasterController instance
        @param      interface    The interface to listen on for SCPI commands
        @param      interface    The port to listen on for SCPI commands
        @param      ioloop       The ioloop to use for async functions

        @note If no IOLoop instance is specified the current instance is used.
        """
        super(EddScpiInterface, self).__init__(interface, port, ioloop)
        self._mc = master_controller
        self._config_file = None

    @scpi_request(str)
    @raise_or_ok
    def request_edd_cmdconfigfile(self, req, filename):
        # not args for FI
        pass

    @scpi_request()
    @raise_or_ok
    def request_edd_start(self, req):
        # not args for FI
        pass

    @scpi_request()
    @raise_or_ok
    def request_edd_stop(self, req):
        # not args for FI
        pass

    @scpi_request()
    @raise_or_ok
    def request_edd_configure(self, req):
        pass

    @scpi_request()
    @raise_or_ok
    def request_edd_abort(self, req):
        pass