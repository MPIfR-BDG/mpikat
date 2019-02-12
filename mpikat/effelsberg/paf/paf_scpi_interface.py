import json
import logging
from tornado.gen import coroutine
from mpikat.core.scpi import ScpiAsyncDeviceServer, scpi_request, raise_or_ok

log = logging.getLogger('mpikat.paf_scpi_interface')

class PafScpiInterface(ScpiAsyncDeviceServer):
    def __init__(self, master_controller, interface, port, ioloop=None):
        """
        @brief      A SCPI interface for a PafMasterController instance

        @param      master_controller   A PafMasterController instance
        @param      interface    The interface to listen on for SCPI commands
        @param      interface    The port to listen on for SCPI commands
        @param      ioloop       The ioloop to use for async functions

        @note If no IOLoop instance is specified the current instance is used.
        """
        super(PafScpiInterface, self).__init__(interface, port, ioloop)
        self._mc = master_controller
        self._config = {}

    @scpi_request(float)
    @raise_or_ok
    def request_pafbe_cmdfrequency(self, req, frequency):
        """
        @brief      Set the centre frequency of the PAF band

        @param      req        An ScpiRequest object
        @param      frequency  The centre frequency of the PAF band in Hz
        """
        self._config['frequency'] = frequency

    @scpi_request(int)
    @raise_or_ok
    def request_pafbe_cmdnbands(self, req, nbands):
        """
        @brief      Set the number of BMF bands to be processed

        @param      req        An ScpiRequest object
        @param      nbands     The number of BMF bands to be processed

        @note Each band corresponds to a 7 x 1 MHz data stream output by
              the PAF beamformer. The maximum number of bands is 48.
        """
        self._config['nbands'] = nbands

    @scpi_request(int)
    @raise_or_ok
    def request_pafbe_cmdbandoffset(self, req, band_offset):
        """
        @brief      Set the index of the first BMF band to be processed

        @param      req          An ScpiRequest object
        @param      band_offset  The index of the first BMF band to be processed

        @note The bands to be processed are required to be contiguous in frequency
              and as such the combination of the bands offset and the number of bands
              must correspond to a contiguous set of bands smaller or equal to the total
              number of bands.
        """
        self._config['band_offset'] = band_offset

    @scpi_request(int)
    @raise_or_ok
    def request_pafbe_cmdnbeams(self, req, nbeams):
        """
        @brief      Set the number of BMF beams to be processed

        @param      req        An ScpiRequest object
        @param      nbeams     The number of BMF beams to be processed

        @note Beams are always selected in ascending order and so nbeams=10
              would correspond to beam indices 0-9.
        """
        self._config['nbeams'] = nbeams

    @scpi_request(str)
    @raise_or_ok
    def request_pafbe_cmdmode(self, req, mode):
        """
        @brief      Set the processing mode to be used on the PAF beams

        @param      req      An ScpiRequest object
        @param      mode     A string denoting the processing mode
        """
        self._config['mode'] = mode

    @scpi_request(str)
    @raise_or_ok
    def request_pafbe_cmdbeamposfile(self, req, filename):
        """
        @brief      Set

        @param      req      An ScpiRequest object
        @param      mode     A
        """
        self._config['beam_pos_fname'] = filename

    @scpi_request(int)
    @raise_or_ok
    def request_pafbe_cmdwritefil(self, req, on):
        """
        @brief      Enable filterbank writing from the PAF processing

        @param      req    An ScpiRequest object
        @param      on     0 = no filterbank writing, 1 = filterbank writing
        """
        self._config['write_filterbank'] = bool(on)

    @scpi_request()
    def request_pafbe_configure(self, req):
        """
        @brief      Configure the PAF backend

        @param      req    An ScpiRequest object
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
            self._mc.configure, json.dumps(self._config)))

    @scpi_request()
    def request_pafbe_start(self, req):
        """
        @brief      Start data processing in the PAF backend

        @param      req    An ScpiRequest object
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
            self._mc.capture_start))

    @scpi_request()
    def request_pafbe_stop(self, req):
        """
        @brief      Stop data processing in the PAF backend

        @param      req    An ScpiRequest object
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
            self._mc.capture_stop))




