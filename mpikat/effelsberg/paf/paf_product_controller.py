"""
Copyright (c) 2018 Ewan Barr <ebarr@mpifr-bonn.mpg.de>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import logging
import json
import astropy.units as units
from astropy.time import Time
from paramiko import PasswordRequiredException
from tornado.gen import coroutine
from mpikat.core.product_controller import ProductController, state_change
from mpikat.effelsberg.paf.routingtable import RoutingTable, RemoteAccess

log = logging.getLogger("mpikat.paf_product_controller")

PAF_WORKER_QUORUM = 0.6

class PafProductStateError(Exception):

    def __init__(self, expected_states, current_state):
        message = ("Possible states for this operation are '{}', "
                   "but current state is '{}'").format(
                   expected_states, current_state)
        super(PafProductStateError, self).__init__(message)


class PafProductError(Exception):
    pass

class PafBeamFileError(Exception):
    pass    


class PafProductController(ProductController):
    """
    Wrapper class for an PAF product.
    """
    def __init__(self, parent, product_id):
        """
        @brief      Construct new instance

        @param      parent            The parent PafMasterController instance
        """
        super(PafProductController, self).__init__(parent, product_id)

    def setup_sensors(self):
        """
        @brief    Setup the default KATCP sensors.

        @note     As this call is made only upon an PAF configure call a mass inform
                  is required to let connected clients know that the proxy interface has
                  changed.
        """
        super(PafProductController, self).setup_sensors()

    @state_change(["capturing", "ready", "error"], "idle")
    @coroutine
    def deconfigure(self):
        """
        @brief      Deconfigure PAF product processing
        """
        log.debug("Deconfiguring product")
        if self.capturing:
            log.debug("Calling capture stop from deconfigure")
            yield self.capture_stop()
        deconfigure_futures = []
        for server in self._servers:
            log.debug("Sending deconfigure request to {}".format(server))
            deconfigure_futures.append(server._client.req.deconfigure())
        for future, server in zip(deconfigure_futures, self._servers):
            log.debug("Awaiting response from {}".format(server))
            result = yield future
            if not result.reply.reply_ok():
                log.error("Failed to deconfigure server {}".format(server))
                log.error("Error message: {}".format(result.reply.arguments))
            else:
                log.debug("Server deconfigured correctly")
        self.teardown_sensors()
        self._parent._server_pool.deallocate(self._servers)
        self._servers = []
        log.debug("Deconfigured product")

    @state_change(["idle", "error"], "ready", "preparing")
    @coroutine
    def configure(self, config_json):
        """
        @brief      Configure PAF product processing

        @param      config_json  A configuration dictionary in JSON format

        @note  The JSON configuration object should be of the form:
               @code
               {
                   "mode": "Search1Beam",
                   "nbands": 48,
                   "frequency": 1340.5,
                   "nbeams": 18,
                   "band_offset": 0,
                   "write_filterbank": 0
               }
               @endcode
        """
        config_dict = json.loads(config_json)
        nservers = self._parent._server_pool.navailable()
        log.info("PAF servers available: {}".format(nservers))
        if nservers == 0:
            raise PafProductError("No servers available for processing")
        log.info("Allocating PAF servers")
        servers = self._parent._server_pool.allocate(nservers)
        log.info("Allocated {} PAF servers".format(len(servers)))
        log.info("Retrieving server capture interfaces")
        destinations = []
        for server in servers:
            log.debug("Retrieving capture interface for server {}".format(server))
            ip = yield server.get_sensor_value("ip")
            mac = yield server.get_sensor_value("mac")
            log.debug("Found IP={}, MAC={}".format(ip, mac))
            destinations.append([mac, ip])
        log.info("Building PAF routing table")
        routing_table = RoutingTable(
            destinations, config_dict['nbeams'],
            config_dict['nbands'], config_dict['band_offset'],
            config_dict['frequency'])
        center_freq = routing_table.center_freq_stream()
        self.log.info(("Uploading routing table for band centred "
                       "at {} MHz").format(center_freq))
        try:
            routing_table.upload_table()
        except PasswordRequiredException:
            log.warning(("Unable to upload routing table due to encrypted key "
                         "(this warning should not exist in production mode)"))
        else:
            log.info("Routing table upload complete")
        be4 = RemoteAccess()
        try:
            be4.connect("134.104.64.134", "obseff")
        except PasswordRequiredException:
            log.warning(("Unable to upload routing table due to encrypted key "
                         "(this warning should not exist in production mode)"))
        try:
            #beam_alt_d, beam_az_d = be4.readfile(config_dict['beam_pos_fname'])
            beam_offset_file = "/home/obseff/paf_test/Scripts/hexpack36"
            beam_az_d, beam_alt_d = be4.readbeamfile(beam_offset_file)

        except PafBeamFileError:
            log.warning("Unable to read beamfile")
        else:
            log.info("Routing table upload complete")
        start_time = Time.now()
        start_time.format = 'isot'
        start_time = start_time + 27.0 * units.s
        config_dict['utc_start_capture'] = start_time.value
        config_dict['beam_alt_d'] = beam_alt_d
        config_dict['beam_az_d'] = beam_az_d
        config_json = json.dumps(config_dict)
        quorum = PAF_WORKER_QUORUM
        failures = 0
        configure_futures = []
        log.info("Configuring all servers")
        for server in servers:
            log.debug("Sending configure request to server {}".format(server))
            configure_futures.append(server._client.req.configure(
                config_json, timeout=60.0))
        for server, future in zip(servers, configure_futures):
            log.debug("Awaiting response from server {}".format(server))
            result = yield future
            if not result.reply.reply_ok():
                log.warning("Failed to configure server {} with error:\n{}".format(
                    server, result.reply.arguments[1]))
                failures += 1
                self._parent._server_pool.deallocate(servers)
            else:
                self._servers.append(server)
        server_str = ",".join(["{s.hostname}:{s.port}".format(
            s=server) for server in self._servers])
        self._servers_sensor.set_value(server_str)
        if 1 - float(failures) / len(servers) < quorum:
            message = ("Failed to reach quorum ({}%), "
                       "{} of {} servers failed to configure"
                       ).format(quorum*100, failures, len(servers))
            log.error(message)
            raise PafProductError(message)
        else:
            log.info("Successfully configured {} of {} servers".format(
                len(self._servers)-failures, len(self._servers)))

    @state_change(["ready", "error"], "capturing", "starting")
    @coroutine
    def capture_start(self, status_json):
        """
        @brief      Start PAF processing

        @param      status_json  A dictionary containing telescope
                                 status information in JSON format
        """
        log.info("Starting product processing")
        status_dict = json.loads(status_json)
        utc_start_process = Time.now()
        utc_start_process.format = 'isot'
        utc_start_process = utc_start_process + 15.0 * units.s
        status_dict['utc_start_process'] = utc_start_process.value
        status_json = json.dumps(status_dict)
        start_futures = []
        for server in self._servers:
            log.debug("Sending start request to server {}".format(server))
            start_futures.append(server._client.req.start(
                status_json, timeout=10.0))
        for future, server in zip(start_futures, self._servers):
            log.debug("Awaiting response from server {}".format(server))
            result = yield future
            if not result.reply.reply_ok():
                message = ("Server {} failed to start "
                           "processing with error:\n{}").format(
                           server, result.reply.arguments)
                log.error(message)
                raise PafProductError(message)
            else:
                log.debug("Server {} started successfully".format(
                    server))
        log.info("Started product successfully")

    @state_change(["capturing", "error"], "ready", "stopping")
    @coroutine
    def capture_stop(self):
        """
        @brief      Stop PAF processing
        """
        log.info("Stopping product processing")
        stop_futures = []
        for server in self._servers:
            log.debug("Sending stop request to server {}".format(server))
            stop_futures.append(server._client.req.stop(timeout=10.0))
        for future, server in zip(stop_futures, self._servers):
            result = yield future
            if not result.reply.reply_ok():
                message = ("Server {} failed to stop "
                           "processing with error:\n{}").format(
                           server, result.reply.arguments)
                log.error(message)
                raise PafProductError(message)
            else:
                log.debug("Server {} stopped successfully".format(
                    server))
        log.info("Stopped product successfully")
