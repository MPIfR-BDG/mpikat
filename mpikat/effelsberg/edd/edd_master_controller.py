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
import coloredlogs
import json

import tornado
import signal

from tornado.gen import Return, coroutine
from katcp import Sensor, AsyncReply
from katcp.kattypes import request, return_reply, Str, Int

from mpikat.effelsberg.edd.edd_roach2_product_controller import ( EddRoach2ProductController)
from mpikat.effelsberg.edd.edd_digpack_client import DigitiserPacketiserClient
from mpikat.effelsberg.edd.edd_fi_client import EddFitsInterfaceClient
from mpikat.effelsberg.edd.edd_server_product_controller import EddServerProductController

import mpikat.effelsberg.edd.pipeline.EDDPipeline as EDDPipeline
import mpikat.effelsberg.edd.EDDDataStore as EDDDataStore

class EddMasterController(EDDPipeline.EDDPipeline):
    """
    The main KATCP interface for the EDD backend
    """
    VERSION_INFO = ("mpikat-edd-api", 0, 2)
    BUILD_INFO = ("mpikat-edd-implementation", 0, 2, "rc1")

    def __init__(self, ip, port, redis_ip, redis_port):
        """
        @brief       Construct new EddMasterController instance

        @params  ip       The IP address on which the server should listen
        @params  port     The port that the server should bind to
        """
        EDDPipeline.EDDPipeline.__init__(self, ip, port)
        self.__controller = {}
        self.__eddDataStore = EDDDataStore.EDDDataStore(redis_ip, redis_port)


    def setup_sensors(self):
        """
        @brief    Set up all sensors on the server

        @note     This is an internal method and is invoked by
                  the constructor of the base class.
        """
        EDDPipeline.EDDPipeline.setup_sensors(self)
        self._edd_config_sensor = Sensor.string(
        "current-config",
        description="The current configuration for the EDD backend",
        default="",
        initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._edd_config_sensor)


    @request()
    @return_reply(Int())
    def request_reset_edd_layout(self, req):
        """
        @brief   Reset the edd layout - after a change of the edd layout via ansible, the iformation about the available products is updated

        @detail  Reread the layout of the edd setup from the ansible data set.
        @return  katcp reply object [[[ !product-list ok | (fail [error description]) <number of configured producers> ]]
        """
        self.__eddDataStore.updateProducts()
        # add a control handle for each product
        #for productid in self.__eddDataStore.products:
        #    product = self.__eddDataStore.getProduct(productid)
        #    # should query the product tog et the right type of controller

        #    self.__controller[productid] = EddServerProductController(self, productid, (product["address"], product["port"]) )

        return ("ok", len(self.__eddDataStore.products))


    @coroutine
    def configure(self, config_json):
        """
        @brief   Configure the EDD backend

        @param   config_json    A JSON dictionary object containing configuration information

        @detail  The configuration dictionary is highly flexible. An example is below:
                 @code
                     {
                         "packetisers":
                         [
                             {
                                 "id": "faraday_room_digitizer",
                                 "address": ["134.104.73.132", 7147],
                                 "sampling_rate": 2600000000.0,
                                 "bit_width": 12,
                                 "v_destinations": "225.0.0.152+3:7148",
                                 "h_destinations": "225.0.0.156+3:7148"
                             }
                         ],
                         "products":
                         [
                             {
                              "id": "GatedSpectrometer",
                               "type": "GatedSpectrometer",
                               "supported_input_formats": {"MPIFR_EDD_Packetizer": [1]},
                               "input_data_streams":
                               {
                                   "polarization_0" :
                                   {
                                       "source": "focus_cabin_digitizer:v_polarization",
                                       "format": "MPIFR_EDD_Packetizer",
                                   },
                                    "polarization_1" :
                                   {
                                       "source": "focus_cabin_digitizer:h_polarization",
                                       "format": "MPIFR_EDD_Packetizer",
                                   }
                               },
                               "output_data_streams":
                               {
                                   "polarization_0" :
                                   {
                                       "format": "MPIFR_EDD_GatedSpectrometer",
                                       "ip": "225.0.0.172 225.0.0.173",
                                       "port": "7152"
                                   },
                                    "polarization_1" :
                                   {
                                       "format": "MPIFR_EDD_GatedSpectrometer",
                                       "ip": "225.0.0.184 225.0.0.185",
                                       "port": "7152"
                                   }
                               },

                               "fft_length": 256,
                               "naccumulate": 32
                             },
                             {
                                 "id": "roach2_spectrometer",
                                 "type": "roach2",
                                 "icom_id": "R2-E01",
                                 "firmware": "EDDFirmware",
                                 "commands":
                                 [
                                     ["program", []],
                                     ["start", []],
                                     ["set_integration_period", [1000.0]],
                                     ["set_destination_address", ["10.10.1.12", 60001]]
                                 ]
                             }
                         ],
                         "fits_interfaces":
                         [
                             {
                                 "id": "fits_interface_01",
                                 "name": "FitsInterface",
                                 "address": ["134.104.73.132", 6000],
                                 "nbeams": 1,
                                 "nchans": 2048,
                                 "integration_time": 1.0,
                                 "blank_phases": 1
                             }
                         ]
                     }
                 @endcode
        """
        log.info("Configuring EDD backend for processing")
        #for i, k in self.__controller.items():
        #    log.debug("Deconfigure existing controller {}".format(i))
        #    k.deconfigure()

        self.__eddDataStore.updateProducts()
        log.info("Resetting data streams")
        #TODo: INterface? Decide if this is always done
        self.__eddDataStore._dataStreams.flushdb()

        log.debug("Received configuration string: '{}'".format(config_json))
        log.debug("Parsing JSON configuration")
        try:
            config = json.loads(config_json)
        except Exception as error:
            log.error("Unable to parse configuration dictionary")
            raise error

        if 'packetisers' in config:
            config['packetizers'] = config.pop('packetisers')
        elif "packetizers" not in config:
            log.warning("No packetizers in config!")
            config["packetizers"] = []

        # Get output streams from packetizer and configure packetizer
        log.info("Configuring digitisers/packetisers")
        for packetizer in config['packetizers']:
            if packetizer["id"] in self.__controller:
                log.debug("Controller for {} already there".format(packetizer["id"]))
            else:
                log.debug("Adding new controller for {}".format(packetizer["id"]))
                self.__controller[packetizer["id"]] = DigitiserPacketiserClient(*packetizer["address"])
                self.__controller[packetizer["id"]].populate_data_store(self.__eddDataStore.host, self.__eddDataStore.port)
            yield self.__controller[packetizer["id"]].configure(packetizer)

            ofs = dict(format="MPIFR_EDD_Packetizer",
                        sample_rate=packetizer["sampling_rate"] / packetizer["predecimation_factor"],
                        bit_depth=packetizer["bit_width"])
            ofs["sync_time"] = yield self.__controller[packetizer["id"]].get_sync_time()
            log.info("Sync Time for {}: {}".format(packetizer["id"], ofs["sync_time"]))

            key = packetizer["id"] + ":" + "v_polarization"
            ofs["ip"] = packetizer["v_destinations"].split(':')[0]
            ofs["port"] = packetizer["v_destinations"].split(':')[1]
            self.__eddDataStore.addDataStream(key, ofs)

            key = packetizer["id"] + ":" + "h_polarization"
            ofs["ip"] = packetizer["h_destinations"].split(':')[0]
            ofs["port"] = packetizer["h_destinations"].split(':')[1]
            self.__eddDataStore.addDataStream(key, ofs)

        log.debug("Identify additional output streams")
        # Get output streams from products
        for product in config['products']:
            for k, i in product["output_data_streams"].iteritems():
                # look up data stream in storage
                dataStream = self.__eddDataStore.getDataFormatDefinition(i['format'])
                dataStream.update(i)
                key = "{}:{}".format(product['id'], k)
                if 'ip' in i:
                    pass
                # ToDo: mark multicast adress as used xyz.mark_used(i['ip'])
                else:
                # ToDo: get MC address automatically if not set
                    raise NotImplementedError("Missing ip statement! Automatic assignment of IPs not implemented yet!")
                self.__eddDataStore.addDataStream(key, i)

        log.debug("Connect data streams with high level description")
        for product in config['products']:
            for k in product["input_data_streams"]:

                datastream = self.__eddDataStore.getDataFormatDefinition(product["input_data_streams"][k]['format'])
                datastream.update(product["input_data_streams"][k])
                if not "source" in product["input_data_streams"][k]:
                    log.debug("Source not definied for input stream {} of {} - no lookup but assuming manual definition!".format(k, product['id']))
                    continue
                s = product["input_data_streams"][k]["source"]

                if not self.__eddDataStore.hasDataStream(s):
                        raise RuntimeError("Unknown data stream {} !".format(s))

                log.debug("Updating {} of {} - with {}".format(k, product['id'], s))
                datastream.update(self.__eddDataStore.getDataStream(s))
                product["input_data_streams"][k] = datastream

        log.debug("Updated configuration:\n '{}'".format(json.dumps(config, indent=2)))

        log.info("Configuring products")
        for product_config in config["products"]:
            product_id = product_config["id"]
            #ToDo : Unify roach2 to bring under ansible control + ServerproductController
            if product_config["type"] == "roach2":
                self._products[product_id] = EddRoach2ProductController(self, product_id,
                                                                        (self._r2rm_host, self._r2rm_port))
            elif product_id not in self.__controller:
                log.warning("Config received for {}, but no controller exists yet.".format(product_id))
                if product_id in self.__eddDataStore.products:
                    product = self.__eddDataStore.getProduct(product_id)
                    self.__controller[product_id] = EddServerProductController(product_id, product["address"], product["port"])
                else:
                    log.warning("Manual config of product {}")
                    self.__controller[product_id] = EddServerProductController(product_id, product_config["address"], product_config["port"])
                self.__controller[packetizer["id"]].populate_data_store(self.__eddDataStore.host, self.__eddDataStore.port)

            yield self.__controller[product_id].configure(product_config)

        # ToDo: Unify FitsInterfaceClient with ServerProductController 
        log.info("Configuring FITS interfaces")
        for fi_config in config["fits_interfaces"]:
            fi = EddFitsInterfaceClient(fi_config["id"], fi_config["address"])
            yield fi.configure(fi_config)
            self.__controller[fi_config["id"]] = fi
        self._edd_config_sensor.set_value(json.dumps(config))
        #self._update_products_sensor()
        log.info("Successfully configured EDD")


    @coroutine
    def deconfigure(self):
        """
        @brief      Deconfigure the EDD backend.
        """
        log.info("Deconfiguring all products:")
        for cid, controller in self.__controller.iteritems():
            logging.debug("  - Deconfigure: {}".format(cid))
            yield controller.deconfigure()
        #self._update_products_sensor()


    @coroutine
    def capture_start(self):
        """
        @brief      Start the EDD backend processing

        @detail     Not all processing components will respond to a capture_start request.
                    For example the packetisers and roach2 boards will in general constantly
                    stream once configured. Processing components (such as the FITS interfaces)
                    which must be cognisant of scan boundaries should respond to this request.

        @note       This method may be updated in future to pass a 'scan configuration' containing
                    source and position information necessary for the population of output file
                    headers.
        """
        for cid, controller in self.__controller.iteritems():
            logging.debug("  - Capture start: {}".format(cid))
            yield controller.capture_start()


    @coroutine
    def capture_stop(self):
        """
        @brief      Stop the EDD backend processing

        @detail     Not all processing components will respond to a capture_stop request.
                    For example the packetisers and roach2 boards will in general constantly
                    stream once configured. Processing components (such as the FITS interfaces)
                    which must be cognisant of scan boundaries should respond to this request.
        """
        for cid, controller in self.__controller.iteritems():
            logging.debug("  - Capture stop: {}".format(cid))
            yield controller.capture_stop()


    @coroutine
    def set(self, config):
        """
        Distribute the settings among the connected components
        """

        for cid, item in config:
            logging.debug("  - Aplying setting: {}:{}".format(cid, item))
            yield self.__controller[cid].set(item)




if __name__ == "__main__":
    parser = EDDPipeline.getArgumentParser()
    parser.add_argument('--redis-ip', dest='redis_ip', type=str, default="localhost",
                      help='The ip for the redis server')
    parser.add_argument('--redis-port', dest='redis_port', type=int, default=6379,
                      help='The port number for the redis server')
    args = parser.parse_args()

    logging.getLogger().addHandler(logging.NullHandler())
    log = logging.getLogger('mpikat')
    log.setLevel(args.log_level.upper())

    log.setLevel(args.log_level.upper())
    coloredlogs.install(
        fmt=("[ %(levelname)s - %(asctime)s - %(name)s "
             "- %(filename)s:%(lineno)s] %(message)s"),
        level=args.log_level.upper(),
        logger=log)
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting Pipeline instance")
    server = EddMasterController(
        args.host, args.port,
        args.redis_ip, args.redis_port)
    log.info("Created Pipeline instance")
    signal.signal(
        signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
            EDDPipeline.on_shutdown, ioloop, server))

    def start_and_display():
        log.info("Starting Pipeline server")
        server.start()
        log.debug("Started Pipeline server")
        log.info(
            "Listening at {0}, Ctrl-C to terminate server".format(
                server.bind_address))

    ioloop.add_callback(start_and_display)
    ioloop.start()
