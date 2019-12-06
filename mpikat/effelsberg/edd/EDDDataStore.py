import redis
import json
import logging

log = logging.getLogger("mpikat.edd_data_store")


class EDDDataStore:
    """
    Interface to the data store for the current EDD configuration
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

        self._ansible.ping()
        self._products.ping()
        self._dataStreams.ping()
        self._telescopeMetaData.ping()


    def updateProducts(self):
        """
        Fill the producers database bsaed on the information in the ansible database
        """
        self._products.flushdb()
        for k in self._ansible.keys():
            if not k.startswith('facts'):
                log.debug("Ignoring: {}".format(k))
                continue
            log.debug("Check facts: {}".format(k))

            facts = json.loads(self._ansible[k])

            ip = facts["ansible_default_ipv4"]

            if 'edd_container'  not in facts:
                log.debug("No products found for: {}".format(k))
                continue

            for p in facts['edd_container']:
                facts['edd_container'][p]['hostname'] = facts['ansible_hostname']
                facts['edd_container'][p]['address'] = facts["ansible_default_ipv4"]['address']
                self._products[p] = json.dumps(facts['edd_container'][p])


    def addDataStream(self, streamid, streamdescription):
        if streamid in self._dataStreams:
            nd = json.dumps(streamdescription)
            if nd == self._dataStreams[streamid]:
                log.warning("Duplicate output streams: {} defined but with same description".format(streamid))
                return
            else:
                log.warning("Duplicate output stream {} defined with conflicting description!\n EXisting description: {}\n New description: {}".format(streamid, self._dataStreams[streamid], nd))
                raise RuntimeError("Invalid configuration")
        self._dataStreams[streamid] = json.dumps(streamdescription)


    def getDataStream(self, streamid):
        return json.loads(self._dataStreams[streamid])

    def hasDataFormatDefinition(self, format_name):
        key = "DataFormats:{}".format(format_name)
        return key in self._edd_static_data

    def getDataFormatDefinition(self, format_name):
        key = "DataFormats:{}".format(format_name)
        if key in self._edd_static_data:
            return json.loads(self._edd_static_data[key])
        else:
            log.warning("Unknown data format: - {}".format(key))
            return {}


    def getProduct(self, productid):
        return json.loads(self._products[productid])


    @property
    def products(self):
        return self._products.keys()


    def hasDataStream(self, streamid):
        return streamid in self._dataStreams

    def addDataFormatDefinition(self, format_name, params):
        """
        """
        key = "DataFormats:{}".format(format_name)
        if isinstance(params, dict):
            params = json.dumps(params)
        log.debug("Add data format definition {} - {}".format(key, params))
        self._edd_static_data[key] = params
