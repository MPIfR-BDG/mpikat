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
import tornado
import time
from tornado.gen import Return, coroutine
from tornado.ioloop import PeriodicCallback
from katcp import Sensor, AsyncDeviceServer
from katcp.kattypes import request, return_reply, Int, Str
from mpikat.core.utils import check_ntp_sync

NTP_CALLBACK_PERIOD = 60 * 5 * 1000 # 5 minutes (in milliseconds)

class ProductLookupError(Exception):
    pass

class ProductExistsError(Exception):
    pass

# ?halt message means shutdown everything and power off all machines
log = logging.getLogger("mpikat.master_controller")

class MasterController(AsyncDeviceServer):
    """This is the main KATCP interface for the FBFUSE
    multi-beam beamformer on MeerKAT.

    This interface satisfies the following ICDs:
    CAM-FBFUSE: <link>
    TUSE-FBFUSE: <link>
    """
    VERSION_INFO = ("mpikat-api", 0, 1)
    BUILD_INFO = ("mpikat-implementation", 0, 1, "rc1")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]
    def __init__(self, ip, port, worker_pool):
        """
        @brief       Construct new MasterController instance

        @params  ip       The IP address on which the server should listen
        @params  port     The port that the server should bind to
        """
        super(MasterController, self).__init__(ip,port)
        self._products = {}
        self._server_pool = worker_pool

    def start(self):
        """
        @brief  Start the MasterController server
        """
        super(MasterController,self).start()

    def add_sensor(self, sensor):
        log.debug("Adding sensor: {}".format(sensor.name))
        super(MasterController, self).add_sensor(sensor)

    def remove_sensor(self, sensor):
        log.debug("Removing sensor: {}".format(sensor.name))
        super(MasterController, self).remove_sensor(sensor)

    def setup_sensors(self):
        """
        @brief  Set up monitoring sensors.

        @note   The following sensors are made available on top of default sensors
                implemented in AsynDeviceServer and its base classes.

                device-status:  Reports the health status of the controller and associated devices:
                                Among other things report HW failure, SW failure and observation failure.

                local-time-synced:  Indicates whether the local time of the servers
                                    is synchronised to the master time reference (use NTP).
                                    This sensor is aggregated from all nodes that are part
                                    of FBF and will return "not sync'd" if any nodes are
                                    unsyncronised.

                products:   The list of product_ids that controller is currently handling
        """
        self._device_status = Sensor.discrete(
            "device-status",
            description="Health status of FBFUSE",
            params=self.DEVICE_STATUSES,
            default="ok",
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._device_status)

        self._local_time_synced = Sensor.boolean(
            "local-time-synced",
            description="Indicates FBF is NTP syncronised.",
            default=True,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._local_time_synced)
        def ntp_callback():
            log.debug("Checking NTP sync")
            try:
                synced = check_ntp_sync()
            except Exception as error:
                log.exception("Unable to check NTP sync")
                self._local_time_synced.set_value(False)
            else:
                if not synced:
                    log.warning("Server is not NTP synced")
                self._local_time_synced.set_value(synced)
        ntp_callback()
        self._ntp_callback = PeriodicCallback(ntp_callback, NTP_CALLBACK_PERIOD)
        self._ntp_callback.start()

        self._products_sensor = Sensor.string(
            "products",
            description="The names of the currently configured products",
            default="",
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._products_sensor)

    def _update_products_sensor(self):
        self._products_sensor.set_value(",".join(self._products.keys()))

    def _get_product(self, product_id):
        if product_id not in self._products:
            raise ProductLookupError("No product configured with ID: {}".format(product_id))
        else:
            return self._products[product_id]

    @request(Str(), Int())
    @return_reply()
    def request_register_worker_server(self, req, hostname, port):
        """
        @brief   Register an WorkerWrapper instance

        @params hostname The hostname for the worker server
        @params port     The port number that the worker server serves on

        @detail  Register an WorkerWrapper instance that can be used for FBFUSE
                 computation. FBFUSE has no preference for the order in which control
                 servers are allocated to a subarray. An WorkerWrapper wraps an atomic
                 unit of compute comprised of one CPU, one GPU and one NIC (i.e. one NUMA
                 node on an FBFUSE compute server).
        """
        log.debug("Received request to register worker server at {}:{}".format(
            hostname, port))
        self._server_pool.add(hostname, port)
        return ("ok",)

    @request(Str(), Int())
    @return_reply()
    def request_deregister_worker_server(self, req, hostname, port):
        """
        @brief   Deregister an WorkerWrapper instance

        @params hostname The hostname for the worker server
        @params port     The port number that the worker server serves on

        @detail  The graceful way of removing a server from rotation. If the server is
                 currently actively processing an exception will be raised.
        """
        log.debug("Received request to deregister worker server at {}:{}".format(
            hostname, port))
        try:
            self._server_pool.remove(hostname, port)
        except ServerDeallocationError as error:
            log.error("Request to deregister worker server at {}:{} failed with error: {}".format(
                hostname, port, str(error)))
            return ("fail", str(error))
        else:
            return ("ok",)

    @request()
    @return_reply(Int())
    def request_worker_server_list(self, req):
        """
        @brief   List all control servers and provide minimal metadata
        """
        for server in self._server_pool.used():
            req.inform("{} allocated".format(server))
        for server in self._server_pool.available():
            req.inform("{} free".format(server))
        return ("ok", len(self._server_pool.used()) + len(self._server_pool.available()))

    @request()
    @return_reply(Int())
    def request_product_list(self, req):
        """
        @brief      List all currently registered products and their states

        @param      req               A katcp request object

        @note       The details of each product are provided via an #inform
                    as a JSON string containing information on the product state.

        @return     katcp reply object [[[ !product-list ok | (fail [error description]) <number of configured products> ]]],
        """
        for product_id,product in self._products.items():
            info = {}
            info[product_id] = product.info()
            as_json = json.dumps(info)
            req.inform(as_json)
        return ("ok",len(self._products))

