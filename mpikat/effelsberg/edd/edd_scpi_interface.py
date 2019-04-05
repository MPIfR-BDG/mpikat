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
        self._config = {}

    @scpi_request(str)
    @raise_or_ok
    def request_eddgsdev_cmdconfigfile(self, req, gitpath):
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
        log.info(
            "Received configuration through SCPI interface:\n{}".format(self._config))
        page.close()

    @scpi_request()
    def request_eddgsdev_configure(self, req):
        """
        @brief      Configure the EDD backend

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:CONFIGURE'
        """
        if self._config == {}:
            raise EddScpiInterfaceError("No configuration set for EDD")
        else:
            self._ioloop.add_callback(self._make_coroutine_wrapper(req,
                                                                   self._mc.configure, self._config))

    @scpi_request()
    def request_eddgsdev_deconfigure(self, req):
        """
        @brief      Configure the EDD backend

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:CONFIGURE'
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
                                                               self._mc.deconfigure))

    @scpi_request(float)
    @raise_or_ok
    def request_eddgsdev_cmdfrequency(self, req, frequency):
        """
        @brief      Set the centre frequency

        @param      req        An ScpiRequest object
        @param      frequency  The centre frequency of the PAF band in Hz
        """
        self._config['frequency'] = frequency

    @scpi_request(float)
    @raise_or_ok
    def request_eddgsdev_cmdinputpower(self, req, input_power):
        """
        @brief      Set the input_power 

        @param      req        An ScpiRequest object
        @param      frequency  The input_power
        """
        self._config['input_power'] = input_power

    @scpi_request(float)
    @raise_or_ok
    def request_eddgsdev_cmdoutputpower(self, req, output_power):
        """
        @brief      Set the output_power 

        @param      req        An ScpiRequest object
        @param      frequency  The output_power
        """
        self._config['output_power'] = output_power

    @scpi_request(float)
    @raise_or_ok
    def request_eddgsdev_cmdintergrationtime(self, req, intergration_time):
        """
        @brief      Set the intergration time

        @param      req        An ScpiRequest object
        @param      frequency  The centre frequency of the PAF band in Hz
        """
        self._config['intergration_time'] = intergration_time

    @scpi_request(float)
    @raise_or_ok
    def request_eddgsdev_cmdnchans(self, req, nchannels):
        """
        @brief      Set the intergration time

        @param      req        An ScpiRequest object
        @param      frequency  The centre frequency of the PAF band in Hz
        """
        self._config['nchannels'] = nchannels

    @scpi_request(float)
    @raise_or_ok
    def request_eddgsdev_cmdfftlength(self, req, fft_length):
        """
        @brief      Set the fftlength

        @param      req        An ScpiRequest object
        @param      frequency  The FFT length
        """
        self._config['fft_length'] = fft_length

    @scpi_request(float)
    @raise_or_ok
    def request_eddgsdev_cmdaccumulationfactor(self, req, accumulation_factor):
        """
        @brief      Set the accumulation factor

        @param      req        An ScpiRequest object
        @param      frequency  The accumulation factor of the FFT
        """
        self._config['accumulation_factor'] = accumulation_factor

    @scpi_request()
    def request_eddgsdev_abort(self, req):
        """
        @brief      Abort EDD backend processing

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:ABORT'
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
                                                               self._mc.capture_stop))

    @scpi_request()
    def request_eddgsdev_start(self, req):
        """
        @brief      Start the EDD backend processing

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:START'
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
                                                               self._mc.capture_start))

    @scpi_request()
    def request_eddgsdev_stop(self, req):
        """
        @brief      Stop the EDD backend processing

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:STOP'
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
                                                               self._mc.capture_stop))
