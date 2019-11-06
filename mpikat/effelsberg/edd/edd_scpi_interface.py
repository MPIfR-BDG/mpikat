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

    def request_edd_gs_cmdconfigfile(self, req, gitpath):
        """
        @brief      Set the configuration file for the EDD

        @param      req       An ScpiRequest object
        @param      gitpath   A git link to a config file as raw user content.
                              For example:
                              https://raw.githubusercontent.com/ewanbarr/mpikat/edd_control/
                              mpikat/effelsberg/edd/config/ubb_spectrometer.json

        @note       Suports SCPI request: 'EDD:CMDCONFIGFILE'
        """
        raise RuntimeError("DEPRECATED: We do not need to load a global config?")
        page = urlopen(gitpath)
        self._config = page.read()
        log.info(
            "Received configuration through SCPI interface:\n{}".format(self._config))
        page.close()

    @scpi_request(str)
    def request_edd_gs_configure(self, req, cfg):
        """
        @brief      Configure the EDD backend

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:CONFIGURE'
        """
        log.debug("Received cfg: {}".format(cfg))

        self._ioloop.add_callback(self._make_coroutine_wrapper(req, self._mc.configure, cfg))

    @scpi_request()
    def request_edd_gs_deconfigure(self, req):
        """
        @brief      Configure the EDD backend

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:CONFIGURE'
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
                                                               self._mc.deconfigure))

#    @scpi_request(float)
#    @raise_or_ok
#    def request_eddgsdev_cmdfrequency(self, req, frequency):
#        """
#        @brief      Set the centre frequency
#
#        @param      req        An ScpiRequest object
#        @param      frequency  The centre frequency of the PAF band in Hz
#        """
#        self._config['frequency'] = frequency
#
#    @scpi_request(int)
#    @raise_or_ok
#    def request_eddgsdev_cmdinputlevel(self, req, input_level):
#        """
#        @brief      Set the input_level
#
#        @param      req        An ScpiRequest object
#        @param      frequency  The input_power
#        """
#        self._config['input_level'] = input_level
#
#    @scpi_request(int)
#    @raise_or_ok
#    def request_eddgsdev_cmdoutputlevel(self, req, output_level):
#        """
#        @brief      Set the output_level
#
#        @param      req        An ScpiRequest object
#        @param      frequency  The output_power
#        """
#        self._config['output_level'] = output_level
#
#    @scpi_request(int)
#    @raise_or_ok
#    def request_eddgsdev_cmdintergrationtime(self, req, intergration_time):
#        """
#        @brief      Set the intergration time
#
#        @param      req        An ScpiRequest object
#        @param      frequency  The centre frequency of the PAF band in Hz
#        """
#        self._config['intergration_time'] = intergration_time
#
#    @scpi_request(int)
#    @raise_or_ok
#    def request_eddgsdev_cmdnchans(self, req, nchannels):
#        """
#        @brief      Set the intergration time
#
#        @param      req        An ScpiRequest object
#        @param      frequency  The centre frequency of the PAF band in Hz
#        """
#        self._config['nchannels'] = nchannels
#
#    @scpi_request(int)
#    @raise_or_ok
#    def request_eddgsdev_cmdfftlength(self, req, fft_length):
#        """
#        @brief      Set the fftlength
#
#        @param      req        An ScpiRequest object
#        @param      frequency  The FFT length
#        """
#        self._config['fft_length'] = fft_length
#
#    @scpi_request(int)
#    @raise_or_ok
#    def request_eddgsdev_cmdnaccumulate(self, req, naccumulate):
#        """
#        @brief      Set the accumulation factor
#
#        @param      req        An ScpiRequest object
#        @param      frequency  The accumulation factor of the FFT
#        """
#        self._config['naccumulate'] = naccumulate
#
#    @scpi_request(int)
#    @raise_or_ok
#    def request_eddgsdev_cmdnbits(self, req, nbits):
#        """
#        @brief      Set the number of bits
#
#        @param      req        An ScpiRequest object
#        @param      bits  the number of bits for the incoming data
#        """
#        self._config['nbits'] = nbits


    @scpi_request()
    def request_edd_gs_abort(self, req):
        """
        @brief      Abort EDD backend processing

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:ABORT'
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
                                                               self._mc.capture_stop))

    @scpi_request()
    def request_edd_gs_start(self, req):
        """
        @brief      Start the EDD backend processing

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:START'
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
                                                               self._mc.capture_start))

    @scpi_request()
    def request_edd_gs_stop(self, req):
        """
        @brief      Stop the EDD backend processing

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:STOP'
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
                                                               self._mc.capture_stop))
