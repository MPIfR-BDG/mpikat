import logging
import time
from tornado.gen import coroutine, sleep, Return
from katcp import KATCPClientResource

log = logging.getLogger("mpikat.edd_digpack_client")

class DigitiserPacketiserError(object):
    pass

class DigitiserPacketiserClient(object):
    def __init__(self, host, port=7147):
        """
        @brief      Class for digitiser packetiser client.

        @param      host   The host IP or name for the desired packetiser KATCP interface
        @param      port   The port number for the desired packetiser KATCP interface
        """
        self._host = host
        self._port = port
        self._client = KATCPClientResource(dict(
            name="digpack-client",
            address=(self._host, self._port),
            controlled=True))
        self._client.start()

    @coroutine
    def _safe_request(self, request_name, *args):
        log.info("Sending packetiser request '{}' with arguments {}".format(request_name, args))
        yield self._client.until_synced()
        response = yield self._client.req[request_name](*args)
        if not response.reply.reply_ok():
            log.error("'{}' request failed with error: {}".format(response.reply.arguments[1]))
            raise DigitiserPacketiserError(response.reply.arguments[1])
        else:
            log.debug("'{}' request successful".format(request_name))
            raise Return(response)

    @coroutine
    def set_sampling_rate(self, rate):
        """
        @brief      Sets the sampling rate.

        @param      rate    The sampling rate in samples per second (e.g. 2.6 GHz should be passed as 2600000000.0)

        @detail     To allow time for reinitialisation of the packetiser firmware during this call we enforce a 10
                    second sleep before the function returns.
        """
        valid_modes = {
            4000000000: ("virtex7_dk769b", "4.0GHz", 5),
            2600000000: ("virtex7_dk769b", "2.6GHz", 3)
        }
        try:
            args = valid_modes[rate]
        except KeyError as error:
            msg = "Invalid sampling rate, valid sampling rates are: {}".format(valid_modes.keys())
            log.error(msg)
            raise DigitiserPacketiserError(msg)
        response = yield self._safe_request("rxs_packetizer_system_reinit", *args)
        yield sleep(10)

    @coroutine
    def set_bit_width(self, nbits):
        """
        @brief      Sets the number of bits per sample out of the packetiser

        @param      nbits  The desired number of bits per sample (e.g. 8 or 12)
        """
        valid_modes = {
            8: "edd08",
            12: "edd12"
        }
        try:
            mode = valid_modes[nbits]
        except KeyError as error:
            msg = "Invalid bit depth, valid bit depths are: {}".format(valid_modes.keys())
            log.error(msg)
            raise DigitiserPacketiserError(msg)
        yield self._safe_request("rxs_packetizer_edd_switchmode", mode)

    @coroutine
    def set_destinations(self, v_dest, h_dest):
        """
        @brief      Sets the multicast destinations for data out of the packetiser

        @param      v_dest  The vertical polarisation channel destinations
        @param      h_dest  The horizontal polarisation channel destinations

        @detail     The destinations should be provided as composite stream definition
                    strings, e.g. 225.0.0.152+3:7148 (this defines four multicast groups:
                    225.0.0.152, 225.0.0.153, 225.0.0.154 and 225.0.0.155, all using
                    port 7148). Currently the packetiser only accepts contiguous IP
                    ranges for each set of destinations.
        """
        yield self._safe_request("capture_destination", "v", v_dest)
        yield self._safe_request("capture_destination", "h", h_dest)

    @coroutine
    def capture_start(self):
        """
        @brief      Start data transmission for both polarisation channels

        @detail     This method uses the packetisers 'capture-start' method
                    which is an aggregate command that ensures all necessary
                    flags on the packetiser and set for data transmission.
                    This includes the 1PPS flag required by the ROACH2 boards.
        """
        yield self._safe_request("capture_start", "vh")

    @coroutine
    def capture_stop(self):
        """
        @brief      Stop data transmission for both polarisation channels
        """
        yield self._safe_request("capture_stop", "vh")

    @coroutine
    def get_sync_time(self):
        """
        @brief      Get the current packetiser synchronisation epoch

        @return     The synchronisation epoch as a unix time float
        """
        response = yield self._safe_request("rxs_packetizer_40g_get_zero_time")
        sync_epoch = float(response.informs[0].arguments[0])
        raise Return(sync_epoch)

    @coroutine
    def synchronize(self, unix_time=None):
        """
        @brief      Set the synchronisation epoch for the packetiser

        @param      unix_time  The unix time to synchronise at. If no value is provided a
                               resonable value will be selected.

        @detail     When explicitly setting the synchronisation time it should be a
                    second or two into the future allow enough time for communication
                    with the packetiser. If the time is in the past by the time the request
                    reaches the packetiser the next 1PPS tick will be selected.
                    Users *must* call get_sync_time to get the actual time that was set.
                    This call will block until the sync epoch has passed (i.e. if a sync epoch
                    is chosen that is 10 second in the future, the call will block for 10 seconds).

        @note       The packetiser rounds to the nearest 1 PPS tick so it is recommended to
                    set the
        """
        if not unix_time:
            unix_time = round(time.time()+2)
        yield self._safe_request("synchronise", 0, unix_time)
        sync_epoch = yield self.get_sync_time()
        if sync_epoch != unix_time:
            log.warning("Requested sync time {} not equal to actual sync time {}".format(unix_time, sync_epoch))


if __name__ == "__main__":
    import coloredlogs
    from optparse import OptionParser
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--host', dest='host', type=str,
        help='Host interface to bind to', default="134.104.73.132")
    parser.add_option('-p', '--port', dest='port', type=long,
        help='Port number to bind to', default=7147)
    parser.add_option('', '--nbits', dest='nbits', type=long,
        help='The number of bits per output sample', default=12)
    parser.add_option('', '--sampling_rate', dest='sampling_rate', type=float,
        help='The digitiser sampling rate (Hz)', default=2600000000.0)
    parser.add_option('', '--v-destinations', dest='v_destinations', type=str,
        help='V polarisation destinations', default="225.0.0.156+3:7148")
    parser.add_option('', '--h-destinations', dest='h_destinations', type=str,
        help='H polarisation destinations', default="225.0.0.156+3:7148")
    parser.add_option('', '--log-level',dest='log_level',type=str,
        help='Logging level',default="INFO")
    (opts, args) = parser.parse_args()
    logging.getLogger().addHandler(logging.NullHandler())
    logger = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level=opts.log_level.upper(),
        logger=logger)
    ioloop = tornado.ioloop.IOLoop.current()
    client = DigitiserPacketiserClient(opts.host, port=opts.port)
    @coroutine
    def configure():
        try:
            yield client.set_sampling_rate(opts.sampling_rate)
            yield client.set_bit_width(opts.nbits)
            yield client.set_destinations(opts.v_destinations, opts.h_destinations)
            yield client.synchronize()
            yield client.capture_start()
        except Exception as error:
            log.error("Error during packetiser configuration: {}".format(str(error)))
            raise error
    ioloop.run_sync(configure)
























