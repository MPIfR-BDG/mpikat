import signal
import logging
import tornado
import types
import pprint
import json
from abc import ABCMeta, abstractmethod
from optparse import OptionParser
from katcp import KATCPClientResource

log = logging.getLogger("r2rm.katcp_to_igui_sidecar")

class KATCPToIGUIConverter(object):

    def __init__(self, host, port):
        """
        @brief      Class for katcp to igui converter.

        @param   host             KATCP host address
        @param   port             KATCP port number
        """
        self.rc = KATCPClientResource(dict(
            name="test-client",
            address=(host, port),
            controlled=True))
        self.host = host
        self.port = port
        self.ioloop = None
        self.ic = None
        self.api_version = None
        self.implementation_version = None
        self.previous_sensors = set()

    def start(self):
        """
        @brief      Start the instance running

        @detail     This call will trigger connection of the KATCPResource client and
                    will login to the iGUI server. Once both connections are established
                    the instance will retrieve a mapping of the iGUI receivers, devices
                    and tasks and will try to identify the parent of the device_id
                    provided in the constructor.

        @param      self  The object

        @return     { description_of_the_return_value }
        """
        @tornado.gen.coroutine
        def _start():
            log.debug("Waiting on synchronisation with server")
            yield self.rc.until_synced()
            log.debug("Client synced")
            log.debug("Requesting version info")
            # This information can be used to get an iGUI device ID
            response = yield self.rc.req.version_list()
            log.info("response {}".format(response))
            # for internal device KATCP server, response.informs[2].arguments return index out of range
            #_, api, implementation = response.informs[2].arguments
            #self.api_version = api
            #self.implementation_version = implementation
            #log.info("katcp-device API: {}".format(self.api_version))
            #log.info("katcp-device implementation: {}".format(self.implementation_version))
            self.ioloop.add_callback(self.update)
        log.debug("Starting {} instance".format(self.__class__.__name__))
        # self.igui_connection.login()
        #self.igui_connection.login(self.igui_user, self.igui_pass)
        # log.debug(self.igui_rxmap)
        # Here we do a look up to find the parent of this device

        #log.debug("iGUI representation:\n{}".format(self.igui_rxmap))
        self.rc.start()
        self.ic = self.rc._inspecting_client
        self.ioloop = self.rc.ioloop
        self.ic.katcp_client.hook_inform("interface-changed",
                                         lambda message: self.ioloop.add_callback(self.update))
        self.ioloop.add_callback(_start)

    @tornado.gen.coroutine
    def update(self):
        """
        @brief    Synchronise with the KATCP servers sensors and register new listners
        """
        log.debug("Waiting on synchronisation with server")
        yield self.rc.until_synced()
        log.debug("Client synced")
        current_sensors = set(self.rc.sensor.keys())
        log.debug("Current sensor set: {}".format(current_sensors))
        removed = self.previous_sensors.difference(current_sensors)
        log.debug("Sensors removed since last update: {}".format(removed))
        added = current_sensors.difference(self.previous_sensors)
        log.debug("Sensors added since last update: {}".format(added))
        for name in list(added):
	    if name == 'observing':
            	log.debug(
                	"Setting sampling strategy and callbacks on sensor '{}'".format(name))
            # strat3 = ('event-rate', 2.0, 3.0)              #event-rate doesn't work
            # self.rc.set_sampling_strategy(name, strat3)    #KATCPSensorError:
            # Error setting strategy
            # not sure that auto means here
            #self.rc.set_sampling_strategy(name, "auto")
            	self.rc.set_sampling_strategy(name, ["period", (1)])
            #self.rc.set_sampling_strategy(name, "event")
            	self.rc.set_sensor_listener(name, self._sensor_updated)
        self.previous_sensors = current_sensors

    def _sensor_updated(self, sensor, reading):
        """
        @brief      Callback to be executed on a sensor being updated

        @param      sensor   The sensor
        @param      reading  The sensor reading
        """
        log.debug("Recieved sensor update for sensor '{}': {}".format(
            sensor.name, repr(reading)))

    def stop(self):
        """
        @brief      Stop the client
        """
        self.rc.stop()

@tornado.gen.coroutine
def on_shutdown(ioloop, client):
    log.info("Shutting down client")
    yield client.stop()
    ioloop.stop()


def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--host', dest='host', type=str,
                      help='The hostname for the KATCP server to connect to')
    parser.add_option('-p', '--port', dest='port', type=long,
                      help='The port number for the KATCP server to connect to')
    parser.add_option('', '--igui_host', dest='igui_host', type=str,
                      help='The hostname of the iGUI interface', default="127.0.0.1")
    parser.add_option('', '--igui_user', dest='igui_user', type=str,
                      help='The username for the iGUI connection')
    parser.add_option('', '--igui_pass', dest='igui_pass', type=str,
                      help='The password for the IGUI connection')
    parser.add_option('', '--igui_device_id', dest='igui_device_id', type=str,
                      help='The iGUI device ID for the managed device')
    parser.add_option('', '--log_level', dest='log_level', type=str,
                      help='Logging level', default="INFO")

    (opts, args) = parser.parse_args()
    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logger = logging.getLogger('r2rm')
    logging.basicConfig(format=FORMAT)
    logger.setLevel(opts.log_level.upper())
    logging.getLogger('katcp').setLevel('INFO')
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting KATCPToIGUIConverter instance")
    client = KATCPToIGUIConverter(opts.host, opts.port)
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, client))

    def start_and_display():
        client.start()
        log.info("Ctrl-C to terminate client")

    ioloop.add_callback(start_and_display)
    ioloop.start()

if __name__ == "__main__":
    main()


