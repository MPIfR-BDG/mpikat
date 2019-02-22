import json
import logging
from urllib2 import urlopen
from tornado.gen import coroutine
from mpikat.core.scpi import ScpiAsyncDeviceServer, scpi_request, raise_or_ok

log = logging.getLogger('mpikat.edd_scpi_interface')

class EddScpiInterfaceError(Exception):
    pass

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
        self._config = None

    @scpi_request(str)
    @raise_or_ok
    def request_edd_cmdconfigfile(self, req, gitpath):
        """
        @brief      Set the configuration file for the EDD

        @param      req       An ScpiRequest object
        @param      gitpath   A git link to a config file as raw user content.
                              For example:
                              https://raw.githubusercontent.com/ewanbarr/mpikat/edd_control/
                              mpikat/effelsberg/edd/config/ubb_spectrometer.json

        @note       Suports SCPI request: 'EDD:CMDCONFIGFILE'
        """
        page = urlopen(gitpath)
        self._config = page.read()
        log.info("Received configuration through SCPI interface:\n{}".format(self._config))
        page.close()

    @scpi_request()
    def request_edd_configure(self, req):
        """
        @brief      Configure the EDD backend

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:CONFIGURE'
        """
        if not self._config:
            raise EddScpiInterfaceError("No configuration set for EDD")
        else:
            self._ioloop.add_callback(self._make_coroutine_wrapper(req,
                self._mc.configure, self._config))

    @scpi_request()
    def request_edd_abort(self, req):
        """
        @brief      Abort EDD backend processing

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:ABORT'
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
            self._mc.capture_stop))

    @scpi_request()
    def request_edd_start(self, req):
        """
        @brief      Start the EDD backend processing

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:START'
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
                self._mc.capture_start))

    @scpi_request()
    def request_edd_stop(self, req):
        """
        @brief      Stop the EDD backend processing

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:STOP'
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
                self._mc.capture_stop))
