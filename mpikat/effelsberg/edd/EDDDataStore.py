import redis
import json
import logging

log = logging.getLogger("mpikat.edd_data_store")


class EDDDataStore:
    """
    @brief Interface to the data store for the EDD.

    @#detail The data store contains the current state of the EDD, augmented
    with additional data of the current state of telescope needed by products.
    """
    def __init__(self, host, port=6379):
        log.debug("Init data store connection: {}:{}".format(host, port))
        self.host = host
        self.port = port

        # The data colelcted by the ansible configuration
        self._ansible = redis.StrictRedis(host=host, port=port, db=0)
        # The currently configured data producers
        self._products = redis.StrictRedis(host=host, port=port, db=1)
        # The currently configured data streams (json objects)
        self._dataStreams = redis.StrictRedis(host=host, port=port, db=2)
        # EDD Static data
        self._edd_static_data = redis.StrictRedis(host=host, port=port, db=3)
        # Telescope meta data
        self._telescopeMetaData = redis.StrictRedis(host=host, port=port, db=4)

        self.__dataBases = [self._ansible, self._products, self._dataStreams, self._edd_static_data, self._telescopeMetaData]
        for d in self.__dataBases:
            d.ping()


    def flush(self):
        """
        @brief Flush content of all databases.
        """
        log.debug("Flushing all databses")
        for d in self.__dataBases:
            d.flushdb()


    def updateProducts(self):
        """
        @brief Fill the producers database based on the information in the ansible database
        """
        self._products.flushdb()
        for k in self._ansible.keys():
            if not k.startswith('facts'):
                log.debug("Ignoring: {}".format(k))
                continue
            log.debug("Check facts: {}".format(k))

            facts = json.loads(self._ansible[k])

            ip = facts["ansible_default_ipv4"]

            if 'edd_container' not in facts or not isinstance(facts['edd_container'], dict):
                log.debug("No products found for: {}".format(k))
                continue

            for p in facts['edd_container']:
                facts['edd_container'][p]['hostname'] = facts['ansible_hostname']
                facts['edd_container'][p]['address'] = facts["ansible_default_ipv4"]['address']
                self._products[p] = json.dumps(facts['edd_container'][p])


    def addDataStream(self, streamid, streamdescription):
        """
        @brief Add a new data stream to the store. Description as dict.
        """
        if streamid in self._dataStreams:
            nd = json.dumps(streamdescription)
            if nd == self._dataStreams[streamid]:
                log.warning("Duplicate output streams: {} defined but with same description".format(streamid))
                returnmpikat/effelsberg/edd/EDDDataStore.py 
            else:
                log.warning("Duplicate output stream {} defined with conflicting description!\n Existing description: {}\n New description: {}".format(streamid, self._dataStreams[streamid], nd))
                raise RuntimeError("Invalid configuration")
        self._dataStreams[streamid] = json.dumps(streamdescription)


    def getDataStream(self, streamid):
        """
        @brief Return data stream with stramid as dict.
        """
        return json.loads(self._dataStreams[streamid])



    def getProduct(self, productid):
        """
        @brief Returns product description as dict.
        """
        return json.loads(self._products[productid])


    @property
    def products(self):
        """
        @brief List of all product ids.
        """
        return self._products.keys()


    def hasDataStream(self, streamid):
        """
        @brief True if data stream with given id exists.
        """
        return streamid in self._dataStreams

    def addDataFormatDefinition(self, format_name, params):
        """
        @brief Adds a new data format description dict to store.
        """
        key = "DataFormats:{}".format(format_name)
        if isinstance(params, dict):
            params = json.dumps(params)
        log.debug("Add data format definition {} - {}".format(key, params))
        self._edd_static_data[key] = params

    def hasDataFormatDefinition(self, format_name):
        """
        @brief Check if data format description already exists.
        """
        key = "DataFormats:{}".format(format_name)
        return key in self._edd_static_data

    def getDataFormatDefinition(self, format_name):
        """
        @brief Returns data format description as dict.
        """
        key = "DataFormats:{}".format(format_name)
        if key in self._edd_static_data:
            return json.loads(self._edd_static_data[key])
        else:
            log.warning("Unknown data format: - {}".format(key))
            return {}
