import logging
import json
from tornado.gen import Return, coroutine
from katcp import Sensor, Message, KATCPClientResource
from mpikat.core.product_controller import ProductController, state_change

log = logging.getLogger("mpikat.edd_server_product_controller")


class EddServerProductController(object):
    """
    """

    def __init__(self, product_id, address, port):
        """
        @brief      Construct new instance

        @param      product_id        A unique identifier for this product
        @param      r2rm_addr         The address of the R2RM (ROACH2 resource manager) to be
                                      used by this product. Passed in tuple format,
                                      e.g. ("127.0.0.1", 5000)
        """
        log.debug("Adress {}, {}".format(address, port))
        self._client = KATCPClientResource(dict(
            name="server-client_{}".format(product_id),
            address=(address, int(port)),
            controlled=True))
        self.__product_id = product_id
        self._client.start()

    @coroutine
    def _safe_request(self, request_name, *args, **kwargs):
        log.info("Sending request '{}' with arguments {}".format(request_name, args))
        yield self._client.until_synced()
        response = yield self._client.req[request_name](*args, **kwargs)
        if not response.reply.reply_ok():
            log.error("'{}' request failed with error: {}".format(request_name, response.reply.arguments[1]))
            raise RuntimeError(response.reply.arguments[1])
        else:
            log.debug("'{}' request successful".format(request_name))
            raise Return(response)

    @coroutine
    def deconfigure(self):
        """
        @brief      Deconfigure the product

        @detail
        """
        yield self._safe_request('deconfigure')

    @coroutine
    def configure(self, config={}):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        logging.debug("Send cfg to {}".format(self.__product_id))
        yield self._safe_request("configure", json.dumps(config), timeout=120.0)

    @coroutine
    def capture_start(self):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        self._safe_request("capture_start")

    @coroutine
    def capture_stop(self):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        self._safe_request("capture_stop")


    @coroutine
    def measurement_start(self):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        self._safe_request("capture_stop")

    @coroutine
    def measurement_stop(self):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        self._safe_request("capture_stop")


    @coroutine
    def set(self, config):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        logging.debug("Send set to {}".format(self.__product_id))
        yield self._safe_request("set", json.dumps(config), timeout=120.0)


