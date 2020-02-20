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
import os

import tornado
import signal

from tornado.gen import Return, coroutine
from katcp import Sensor, AsyncDeviceServer, AsyncReply, FailReply
from katcp.kattypes import request, return_reply, Str, Int

from mpikat.effelsberg.edd.edd_roach2_product_controller import ( EddRoach2ProductController)
from mpikat.effelsberg.edd.edd_digpack_client import DigitiserPacketiserClient
from mpikat.effelsberg.edd.edd_server_product_controller import EddServerProductController

from mpikat.utils.process_tools import ManagedProcess, command_watcher
import mpikat.effelsberg.edd.pipeline.EDDPipeline as EDDPipeline
import mpikat.effelsberg.edd.EDDDataStore as EDDDataStore

log = logging.getLogger("mpikat.effelsberg.edd.EddMAsterController")

class EddMasterController(EDDPipeline.EDDPipeline):
    """
    The main KATCP interface for the EDD backend
    """
    VERSION_INFO = ("mpikat-edd-api", 0, 2)
    BUILD_INFO = ("mpikat-edd-implementation", 0, 2, "rc1")

    def __init__(self, ip, port, redis_ip, redis_port, edd_ansible_git_repository_folder):
        """
        @brief       Construct new EddMasterController instance

        @params  ip           The IP address on which the server should listen
        @params  port         The port that the server should bind to
        @params  redis_ip     IP for conenction to the EDD Datastore
        @params  redis_port   Port for the comnenctioon to the edd data store
        @params  edd_ansible_git_repository_folder
                              Directory of a (checked out) edd_ansible git
                              repository to be used for provisioning
        """
        EDDPipeline.EDDPipeline.__init__(self, ip, port)
        self.__controller = {}
        self.__eddDataStore = EDDDataStore.EDDDataStore(redis_ip, redis_port)
        self.__edd_ansible_git_repository_folder = edd_ansible_git_repository_folder
        if not os.path.isdir(self.__edd_ansible_git_repository_folder):
            log.warning("{} is not a readable directory".format(self.__edd_ansible_git_repository_folder))

        self.__provisioned = None

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
    def set(self, config_json):
        try:
            cfg = json.loads(config_json)
        except:
            log.error("Error parsing json")
            raise FailReply("Cannot handle config string {} - Not valid json!".format(config_json))
        if 'packetisers' in cfg:
            cfg['packetizers'] = cfg.pop('packetisers')

        EDDPipeline.EDDPipeline.set(self, cfg)



    @coroutine
    def configure(self, config_json):
        """
        @brief   Configure the EDD backend

        @param   config_json    A JSON dictionary object containing configuration information

        """
        log.info("Configuring EDD backend for processing")

        self.__eddDataStore.updateProducts()
        log.info("Resetting data streams")
        #TODo: INterface? Decide if this is always done
        self.__eddDataStore._dataStreams.flushdb()

        log.debug("Received configuration string: '{}'".format(config_json))

        yield self.set(config_json)
        cfs = json.dumps(self._config, indent=4)
        log.info("Final configuration:\n" + cfs)

        if "packetizers" not in self._config:
            log.warning("No packetizers in config!")
            self._config["packetizers"] = []

        if not 'products' in self._config:
            log.warning("No products in config!")
            self._config["products"] = []

        # ToDo: Check if provisioned
        if not self.__provisoned:
            self._installController(self._config)


        # Data streams are only filled in on final configure as they may
        # require data from the configure command. As example,t he packetizer
        # dat atream has a sync time that is propagated to other components

        # Get output streams from packetizer and configure packetizer
        log.info("Configuring digitisers/packetisers")
        for packetizer in self._config['packetizers']:
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
            yield self.__controller[packetizer["id"]].populate_data_store(self.__eddDataStore.host, self.__eddDataStore.port)

        log.debug("Identify additional output streams")
        # Get output streams from products
        for product in self._config['products']:
            if not "output_data_streams" in product:
                continue

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
        for product in self._config['products']:
            counter = 0
            for k in product["input_data_streams"]:
                if isinstance(product["input_data_streams"], dict):
                    inputStream = product["input_data_streams"][k]
                elif isinstance(product["input_data_streams"], list):
                    inputStream = k
                    k = counter
                    counter += 1
                else:
                    raise RuntimeError("Input streams has to be dict ofr list, got: {}!".format(type(product["input_data_streams"])))

                datastream = self.__eddDataStore.getDataFormatDefinition(inputStream['format'])
                datastream.update(inputStream)
                if not "source" in inputStream:
                    log.debug("Source not definied for input stream {} of {} - no lookup but assuming manual definition!".format(k, product['id']))
                    continue
                s = inputStream["source"]

                if not self.__eddDataStore.hasDataStream(s):
                        raise RuntimeError("Unknown data stream {} !".format(s))

                log.debug("Updating {} of {} - with {}".format(k, product['id'], s))
                datastream.update(self.__eddDataStore.getDataStream(s))
                product["input_data_streams"][k] = datastream

        log.debug("Updated configuration:\n '{}'".format(json.dumps(self._config, indent=2)))
        log.info("Configuring products")
        for product_config in self._config["products"]:
            yield self.__controller[product_id].configure(product_config)

        self._edd_config_sensor.set_value(json.dumps(self._config))
        log.info("Successfully configured EDD")
        raise Return("Successfully configured EDD")


    @coroutine
    def deconfigure(self):
        """
        @brief      Deconfigure the EDD backend.
        """
        log.info("Deconfiguring all products:")
        for cid, controller in self.__controller.iteritems():
            log.debug("  - Deconfigure: {}".format(cid))
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
            log.debug("  - Capture start: {}".format(cid))
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
            log.debug("  - Capture stop: {}".format(cid))
            yield controller.capture_stop()


    @coroutine
    def measurement_start(self):
        """
        """
        for cid, controller in self.__controller.iteritems():
            log.debug("  - Measurement start: {}".format(cid))
            yield controller.measurement_start()


    @coroutine
    def measurement_stop(self):
        """
        """
        for cid, controller in self.__controller.iteritems():
            log.debug("  - Measurement stop: {}".format(cid))
            yield controller.measurement_stop()


    def reset(self):
        """
        Resets the EDD, i.e. flusing all data bases. Note that runing containers are not stopped.
        """
        self.__eddDataStore._dataStreams.flushdb()


    @request(Str())
    @return_reply()
    def request_provision(self, req, name):
        """
        @brief   Loads a provision configuration and dispatch it to ansible and sets the data streams for all products

        """
        @coroutine
        def wrapper():
            try:
                yield self.provision(name)
            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(wrapper)
        raise AsyncReply


    @coroutine
    def provision(self, description):
        """
        @brief provision the edd with provided provision description.

        @params description fo the provision. This has to be a string of format
                NAME                 to load NAME.json and NAME.yml
                NAME1.yml;NAME2.json to load different yml / json configs
        """

        os.chdir(self.__edd_ansible_git_repository_folder)
        log.debug("Provision description {} from directory {}".format(description, os.getcwd()))
        if description.startswith('"'):
            description = description.lstrip('"')
            description = description.rstrip('"')

        descr_subfolder = "provison_descriptions"
        if ";" in description:
            description = description.split(';')
            if description[0].endswith(yml):
                playbook_file = os.path.join(descr_subfolder, description[0])
                basic_config_file = os.path.join(descr_subfolder, description[1])
            else:
                playbook_file = os.path.join(descr_subfolder, description[1])
                basic_config_file = os.path.join(descr_subfolder, description[0])
        else:
            playbook_file = os.path.join(descr_subfolder, description + ".yml")
            basic_config_file = os.path.join(descr_subfolder, description + ".json")

        if not os.path.isfile(playbook_file):
            raise FailReply("cannot find playbook file {}".format(playbook_file))
        if not os.path.isfile(basic_config_file):
            raise FailReply("cannot find config file {}".format(basic_config_file))

        log.debug("Loading provision description files: {} and {}".format(playbook_file, basic_config_file))
        try:
            yield command_watcher("ansible-playbook {}".format(playbook_file),
                    env={"ANSIBLE_ROLES_PATH":os.path.join(self.__edd_ansible_git_repository_folder,
                        "roles")})
        except Exception as E:
            raise FailReply("Error in provisioning {}".format(E))
        self.__provisioned = playbook_file

        try:
            with open(basic_config_file) as cfg:
                basic_config = json.load(cfg)
        except Exception as E:
            raise FailReply("Error reading config {}".format(E))

        self.__eddDataStore.updateProducts()
        self._installController(basic_config)

        # Retrieve default configs from products and merge with basic config to
        # have full config locally.
        self._config = {}
        for product in basic_config['products']:
            log.debug("Retrieve basic config for {}".format(product["id"]))
            controller = self.__controller[product["id"]]
            if not isinstance(controller, EddServerProductController):
                # ToDo: unfify interface of DigitiserPacketiserClient and EDD Pipeline,
                # they should be identical. Here the DigitiserPacketiserClient
                # should mimick the getConfig and translate it as the actual
                # request is not there
                continue

            cfg = yield controller.getConfig()
            cfg = EDDPipeline.updateConfig(cfg, product)
            self._config[product["id"]] = cfg
        self._configUpdated()


    def _installController(self, config):
        """
        Ensure a controller exists for all components in a configuration
        """
        log.debug("Installing controller for products.")
        # Install controllers
        if 'packetisers' in config:
            config['packetizers'] = config.pop('packetisers')
        elif "packetizers" not in config:
            log.warning("No packetizers in config!")
            config["packetizers"] = []
        if not 'products' in config:
            config["products"] = []

        for packetizer in config['packetizers']:
            if packetizer["id"] in self.__controller:
                log.debug("Controller for {} already there".format(packetizer["id"]))
            else:
                log.debug("Adding new controller for {}".format(packetizer["id"]))
                self.__controller[packetizer["id"]] = DigitiserPacketiserClient(*packetizer["address"])
                self.__controller[packetizer["id"]].populate_data_store(self.__eddDataStore.host, self.__eddDataStore.port)

        for product_config in config["products"]:
            product_id = product_config["id"]
            if product_id in self.__controller:
                log.debug("Controller for {} already there".format(product_id))
            elif "type" in product_config and product_config["type"] == "roach2":
                    self._products[product_id] = EddRoach2ProductController(self, product_id,
                                                                            (self._r2rm_host, self._r2rm_port))
            elif product_id not in self.__controller:
                if product_id in self.__eddDataStore.products:
                    product = self.__eddDataStore.getProduct(product_id)
                    self.__controller[product_id] = EddServerProductController(product_id, product["address"], product["port"])
                else:
                    log.warning("Manual setup of product {} - require address and port properties")
                    self.__controller[product_id] = EddServerProductController(product_id, product_config["address"], product_config["port"])


    @request()
    @return_reply()
    def request_deprovision(self, req):
        """
        @brief   Deprovision EDD - stop all ansible containers launched in recent provision cycle.
        """
        @coroutine
        def wrapper():
            try:
                yield self.deprovision()
            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(wrapper)
        raise AsyncReply


    @coroutine
    def deprovision(self):
        log.debug("Deprovision {}".format(self.__provisioned))
        try:
            yield command_watcher("ansible-playbook {} --tags=stop".format(self.__provisioned),
                    env={"ANSIBLE_ROLES_PATH":os.path.join(self.__edd_ansible_git_repository_folder,
                        "roles")})

        except Exception as E:
            raise FailReply("Error in provisioning {}".format(E))
        self.__provisioned = None
        self.__eddDataStore.updateProducts()
        self.__eddDataStore._dataStreams.flushdb()



if __name__ == "__main__":
    parser = EDDPipeline.getArgumentParser()
    parser.add_argument('--redis-ip', dest='redis_ip', type=str, default="localhost",
                      help='The ip for the redis server')
    parser.add_argument('--redis-port', dest='redis_port', type=int, default=6379,
                      help='The port number for the redis server')

    parser.add_argument('--edd_ansible_repository', dest='edd_ansible_git_repository_folder', type=str, default=os.path.join(os.getenv("HOME"), "edd_ansible"), help='The path to a git repository for the provisioning data')
    args = parser.parse_args()

    server = EddMasterController(
        args.host, args.port,
        args.redis_ip, args.redis_port, args.edd_ansible_git_repository_folder)
    EDDPipeline.launchPipelineServer(server, args)
