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

        log.info("Master ast {}:{}".format(master_ip, master_port))
        super(EddScpiInterface, self).__init__(interface, port, ioloop)
        self.address = (master_ip, master_port)
        self.__controller = EddServerProductController("MASTER", master_ip, master_port)


    @scpi_request()
    @raise_or_ok
    @coroutine
    def request_edd_configure(self, req):
        """
        @brief      Configure the EDD backend

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:CONFIGURE'
        """
        self.__controller.configure()
        #self.__controller.capture_start()

    @scpi_request()
    @raise_or_ok
    @coroutine
    def request_edd_deconfigure(self, req):
        """
        @brief      Deconfigure the EDD backend

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:DECONFIGURE'
        """
        self.__controller.deconfigure()

    @scpi_request()
    @raise_or_ok
    @coroutine
    def request_edd_start(self, req):
        """
        @brief      Start the EDD backend processing

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:START'
        """
        self.__controller.measurement_start()

    @scpi_request()
    @raise_or_ok
    @coroutine
    def request_edd_stop(self, req):
        """
        @brief      Stop the EDD backend processing

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:STOP'
        """
        self.__controller.measurement_stop()

    @scpi_request(str)
    @raise_or_ok
    @coroutine
    def request_edd_set(self, req, message):
        """
        @brief     Set an option for an edd backend component.

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:SET ID:OPTION VALUE'
        """
        # json from message with unravvelled colons
        d = {}
        g = d
        p, o  = message.split()
        for i in p.split(':')[:-1]:
            d[i] = {}
            d = d[i]
        d[p[-1]] = o
        self.__controller.set(g)

    @scpi_request(str)
    def request_edd_construct(self, req, message):
        req.error("NOT IMPLEMENTED YET")

    @scpi_request()
    def request_edd_teardown(self, req, message):
        req.error("NOT IMPLEMENTED YET")


if __name__ == "__main__":

    parser = EDDPipeline.getArgumentParser()
    parser.add_argument('--master-controller-ip', dest='master_ip', type=str, default="edd01",
                      help='The ip for the master controller')
    parser.add_argument('--master-controller-port', dest='master_port', type=int, default=7147,
                      help='The port number for the master controller')
    args = parser.parse_args()

    logging.getLogger().addHandler(logging.NullHandler())
    log = logging.getLogger('mpikat')
    log.setLevel(args.log_level.upper())
    coloredlogs.install(
        fmt=("[ %(levelname)s - %(asctime)s - %(name)s "
             "- %(filename)s:%(lineno)s] %(message)s"),
        level=args.log_level.upper(),
        logger=log)

    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting Pipeline instance")
    server = EddScpiInterface(args.host, args.port, args.master_ip, args.master_port)
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
