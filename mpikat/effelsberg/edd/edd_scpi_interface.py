import json
import logging
from mpikat.core.scpi import ScpiAsyncDeviceServer, scpi_request, raise_or_ok, launch_server
import mpikat.effelsberg.edd.pipeline.EDDPipeline as EDDPipeline
from mpikat.effelsberg.edd.edd_server_product_controller import EddServerProductController
import coloredlogs
from tornado.gen import Return, coroutine, sleep
import tornado
import signal

log = logging.getLogger('mpikat.edd_scpi_interface')



class EddScpiInterface(ScpiAsyncDeviceServer):
    def __init__(self, interface, port, master_ip, master_port, ioloop=None):
        """
        @brief      A SCPI interface for a EddMasterController instance

        @param      master_controller_ip    IP of a master controll to send commands to
        @param      master_controller_port  Port of a master controll to send commands to
        @param      interface    The interface to listen on for SCPI commands
        @param      port        The port to listen on for SCPI commands
        @param      ioloop       The ioloop to use for async functionsnsible apply roles round robin

        @note If no IOLoop instance is specified the current instance is used.
        """

        log.info("Listening at {}:{}".format(interface, port))
        log.info("Master at {}:{}".format(master_ip, master_port))
        super(EddScpiInterface, self).__init__(interface, port, ioloop)
        self.address = (master_ip, master_port)
        self.__controller = EddServerProductController("MASTER", master_ip, master_port)


    @scpi_request()
    def request_edd_configure(self, req):
        """
        @brief      Configure the EDD backend

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:CONFIGURE'
        """

        @coroutine
        def wrapper():
            try:
                F = self.__controller.configure()

                @coroutine
                def cb(f):
                    yield self.__controller.capture_start()
                F.add_done_callback(cb)
                yield F
            except Exception as E:
                log.error(E)
                req.error(E)
            else:
                req.ok()
        self._ioloop.add_callback(wrapper)


    @scpi_request()
    def request_edd_deconfigure(self, req):
        """
        @brief      Deconfigure the EDD backend

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:DECONFIGURE'
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req, self.__controller.deconfigure))


    @scpi_request()
    def request_edd_start(self, req):
        """
        @brief      Start the EDD backend processing

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:START'
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req, self.__controller.measurement_start))


    @scpi_request()
    def request_edd_stop(self, req):
        """
        @brief      Stop the EDD backend processing

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:STOP'
        """
        self.__controller.measurement_stop()
        self._ioloop.add_callback(self._make_coroutine_wrapper(req, self.__controller.measurement_stop))


    @scpi_request(str, str)
    def request_edd_set(self, req, product_option, value):
        """
        @brief     Set an option for an edd backend component.

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:SET ID:OPTION VALUE'
                    VALUE needs to be valid json, i.e. strings are marked with ""
        """
        log.debug(" Received {} {}".format(product_option, value))
        # Create nested option dict from colon seperateds tring
        try:
            d = {}
            g = d
            option_list = product_option.split(':')
            for el in option_list[:-1]:
                d[el] = {}
                d = d[el]
            d[option_list[-1]] = json.loads(value)
        except Exception as E:
            em = "Error parsing command: {} {}\n{}".format(product_option, value, E)
            log.error(em)
            req.error(em)
        else:
            log.debug(" - {}".format(g))
            self._ioloop.add_callback(self._make_coroutine_wrapper(req, self.__controller.set, g))


    @scpi_request(str)
    def request_edd_provision(self, req, message):
        self._ioloop.add_callback(self._make_coroutine_wrapper(req, self.__controller.provision, message))


    @scpi_request()
    def request_edd_deprovision(self, req):
        self._ioloop.add_callback(self._make_coroutine_wrapper(req, self.__controller.deprovision))


if __name__ == "__main__":

    parser = EDDPipeline.getArgumentParser()
    parser.add_argument('--master-controller-ip', dest='master_ip', type=str, default="edd01",
                      help='The ip for the master controller')
    parser.add_argument('--master-controller-port', dest='master_port', type=int, default=7147,
                      help='The port number for the master controller')
    args = parser.parse_args()

    EDDPipeline.setup_logger(log, args.log_level.upper())

    server = EddScpiInterface(args.host, args.port, args.master_ip, args.master_port)
    #Scpi Server is not an EDDPipieline, but launcher work nevertheless
    EDDPipeline.launchPipelineServer(server, args)



