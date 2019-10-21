import logging
from tornado.gen import coroutine
from katcp import Sensor, Message, KATCPClientResource
from mpikat.core.product_controller import ProductController, state_change

log = logging.getLogger("mpikat.edd_server_product_controller")


class EddServerProductController(ProductController):
    """
    """

    def __init__(self, parent, product_id, r2rm_addr):
        """
        @brief      Construct new instance

        @param      parent            The parent EddRoach2MasterController instance
        @param      product_id        A unique identifier for this product
        @param      r2rm_addr         The address of the R2RM (ROACH2 resource manager) to be
                                      used by this product. Passed in tuple format,
                                      e.g. ("127.0.0.1", 5000)
        """
        ProductController.__init__(self, parent, product_id)
        self._client = KATCPClientResource(dict(
            name="r2rm-client",
            address=r2rm_addr,
            controlled=True))
        self._client.start()

    def setup_sensors(self):
        """
        @brief    Setup the default KATCP sensors.

        @note     As this call is made only upon an EDD product configure call a mass inform
                  is required to let connected clients know that the proxy interface has
                  changed.
        """
        ProductController.setup_sensors(self)
        self._dummy_sensor = Sensor.string(
            "dummy-sensor",
            description="dummy sensor as alpha version of code",
            default="ALPHA VERSION OF CODE, NO MORE SENSORS HERE",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._dummy_sensor)
        self._parent.mass_inform(Message.inform('interface-changed'))

    @state_change(["capturing", "error"], "idle")
    @coroutine
    def deconfigure(self):
        """
        @brief      Deconfigure the product

        @detail     This method will remove any product sensors that were added to the
                    parent master controller.
        """
        yield self._client.deconfigure()

    @state_change(["idle", "error"], "capturing", "preparing")
    @coroutine
    def configure(self, config):
        """
        @brief      Configure the roach2 product

        @param      config  A dictionary containing configuration information.
                            The dictionary should have a form similar to:
                            @code
                                 {
                                     "id": "roach2_spectrometer",
                                     "type": "roach2",
                                     "icom_id": "R2-EDD",
                                     "firmware": "EDDFirmware",
                                     "commands":
                                     [
                                         ["program", []],
                                         ["start", []],
                                         ["set_integration_period", [1000.0]],
                                         ["set_destination_address", ["10.10.1.12", 60001]]
                                     ]
                                 }
                            @endcode

        @detail  This method will request the specified roach2 board from the R2RM server
                 and request a firmware deployment. The values of the 'icom_id' and 'firmware'
                 must correspond to valid managed roach2 boards and firmwares as understood by
                 the R2RM server.
        """
        self._client.configure(config)

    @coroutine
    def capture_start(self):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        self._client.capture_start()

    @coroutine
    def capture_stop(self):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        self._client.capture_stop()
