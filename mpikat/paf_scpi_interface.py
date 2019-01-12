from mpikat.scpi import AsyncScpiDeviceServer

class PafScpiInterface(ScpiAsyncDeviceServer):
    def __init__(self, master_controller, interface, port, ioloop=None):
        super(PafScpiInterface, self).__init__(interface, port, ioloop)
        self._mc = master_controller
        self._config = {}

    @scpi_request(float)
    @raise_or_ok
    def request_pafbe_setfrequency(self, req, frequency):
        self._config['frequency'] = frequency

    @scpi_request(int)
    @raise_or_ok
    def request_pafbe_setnbands(self, req, nbands):
        self._config['nbands'] = nbands

    @scpi_request(int)
    @raise_or_ok
    def request_pafbe_setbandoffset(self, req, band_offset):
        self._config['band_offset'] = band_offset

    @scpi_request(int)
    @raise_or_ok
    def request_pafbe_setnbeams(self, req, nbeams):
        self._config['nbeams'] = nbeams

    @scpi_request(str)
    @raise_or_ok
    def request_pafbe_setmode(self, req, mode):
        self._config['mode'] = mode

    @scpi_request(str)
    @raise_or_ok
    def request_pafbe_setwritefil(self, req, write_filterbank):
        self._config['write_filterbank'] = write_filterbank

    def _make_coroutine_wrapper(self, req, cr, *args, **kwargs):
        @coroutine
        def wrapper():
            try:
                yield cr(*args, **kwargs)
            except Exception as error:
                log.error(str(error))
                req.error(str(error))
                raise error
            else:
                req.ok()
        return wrapper

    @scpi_request()
    def request_pafbe_configure(self, req):
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
            self._mc.configure, json.dumps(self._config)))

    @scpi_request()
    def request_pafbe_start(self, req):
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
            self._mc.capture_start))

    @scpi_request()
    def request_pafbe_stop(self, req):
        self._ioloop.add_callback(self._make_coroutine_wrapper(req,
            self._mc.capture_stop))




