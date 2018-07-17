import logging
import json
import tornado
import signal
from optparse import OptionParser
from katcp import Sensor, Message, AsyncDeviceServer
from katcp.kattypes import request, return_reply, Str
from katportalclient import KATPortalClient
from katpoint import Antenna, Target

FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=FORMAT)
log = logging.getLogger("mpikat.fbfuse_ca_server")

class FbfConfigurationAuthority(AsyncDeviceServer):
    """This is an example/template for how users
    may develop an fbf configuration authority server
    """
    VERSION_INFO = ("mpikat-fbf-ca-api", 0, 1)
    BUILD_INFO = ("mpikat-fbf-ca-implementation", 0, 1, "rc1")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]
    def __init__(self, ip, port):
        super(FbfConfigurationAuthority, self).__init__(ip,port)
        self._products = {}

    def start(self):
        """
        @brief  Start the FbfConfigurationAuthority server
        """
        super(FbfConfigurationAuthority,self).start()

    def setup_sensors(self):
        """
        @brief  Set up monitoring sensors.

        @note   The following sensors are made available on top of default sensors
                implemented in AsynDeviceServer and its base classes.

                device-status:  Reports the health status of the CA server and associated devices:
                                Among other things report HW failure, SW failure and observation failure.

                products:   The list of product_ids that the CA server is currently handling
        """
        self._device_status = Sensor.discrete(
            "device-status",
            description="Health status of this device",
            params=self.DEVICE_STATUSES,
            default="ok",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._device_status)

        self._configuration_sensors = {}

    @request(Str(), Str())
    @return_reply(Str())
    def request_get_schedule_block_configuration(self, req, product_id, sb_id):
        """
        @brief      Get an FBFUSE configuration for the current instance

        @param      product_id  The product identifier
        @param      sb_id       The schedule block identifier

        @note       The product_id argument may be superfluous, although it allows
                    the CA server to look up parameters on the unconfigured product
                    from the FBFUSE sensor set.
        """
        # do something clever with product_id and sb_id
        # probably boot up katportalclient

        if product_id in self._configuration_sensors:
            self.remove_sensor(self._configuration_sensors[product_id])
            del self._configuration_sensors[product_id]
        self.mass_inform(Message.inform('interface-changed'))
        config = {u'coherent-beams':
                     {u'antennas': u'm007,m008,m009,m010',
                      u'fscrunch': 16,
                      u'nbeams': 100,
                      u'tscrunch': 16},
                  u'incoherent-beam':
                     {u'antennas': u'm007,m008,m009,m010',
                     u'fscrunch': 16,
                     u'tscrunch': 1}}
        return ("ok", json.dumps(config))

    @request(Str(), Str())
    @return_reply()
    def request_target_configuration_start(self, req, product_id, target_string):
        """
        @brief      Set up a beam configuration sensor for the FBFUSE instance

        @param      product_id     The product identifier
        @param      target_string  A KATPOINT target string (boresight pointing position)
        """
        if not product_id in self._configuration_sensors:
            self._configuration_sensors[product_id] = Sensor.string(
                "{}-beam-position-configuration".format(product_id),
                description="Configuration description for FBF beam placement",
                default="",
                initial_status=Sensor.NOMINAL)
            self.add_sensor(self._configuration_sensors[product_id])
            self.mass_inform(Message.inform('interface-changed'))

        config = {'beams': ['PSRJ1733+1010,radec,17:33:00,10:10:00',
            'PSRJ1100+1010,radec,11:00:00,10:10:00'],
            'tilings': [{'epoch': 1531844084.878784,
            'nbeams': 30,
            'overlap': 0.5,
            'reference_frequency': 1.4e9,
            'target': 'laduma_field_0,radec,11:00:00,10:10:00'}]}
        self._configuration_sensors[product_id].set_value(json.dumps(config))
        # Updates to this sensor will trigger a change in beam configuration on
        # FBFUSE
        return ("ok",)

@tornado.gen.coroutine
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
    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    log.setLevel(opts.log_level.upper())
    for handler in logging.getLogger('').handlers:
        handler.setFormatter(logging.Formatter(FORMAT))
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting FbfConfigurationAuthority instance")
    server = FbfConfigurationAuthority(opts.host, opts.port)
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, server))
    def start_and_display():
        server.start()
        log.info("Listening at {0}, Ctrl-C to terminate server".format(server.bind_address))
    ioloop.add_callback(start_and_display)
    ioloop.start()

if __name__ == "__main__":
    main()












