import logging
import json
import tornado
import signal
import time
import numpy as np
import ipaddress
import mosaic
from threading import Lock
from optparse import OptionParser
from tornado.gen import Return, coroutine
from katcp import Sensor, Message, AsyncDeviceServer, KATCPClientResource, AsyncReply
from katcp.kattypes import request, return_reply, Int, Str, Discrete, Float
from katportalclient import KATPortalClient
from katpoint import Antenna, Target


# ?halt message means shutdown everything and power off all machines


log = logging.getLogger("mpikat.fbfuse")

lock = Lock()

PORTAL = "monctl.devnmk.camlab.kat.ac.za"

FBF_IP_RANGE = "spead://239.11.1.0+127:7147"

###################
# Utility functions
###################

def is_power_of_two(n):
    """
    @brief  Test if number is a power of two

    @return True|False
    """
    return n != 0 and ((n & (n - 1)) == 0)

def next_power_of_two(n):
    """
    @brief  Round a number up to the next power of two
    """
    return 2**(n-1).bit_length()

def parse_csv_antennas(antennas_csv):
    antennas = antennas_csv.split(",")
    nantennas = len(antennas)
    if nantennas == 1 and antennas[0] == '':
        raise AntennaValidationError("Provided antenna list was empty")
    names = [antenna.strip() for antenna in antennas]
    if len(names) != len(set(names)):
        raise AntennaValidationError("Not all provided antennas were unqiue")
    return names

def ip_range_from_stream(stream):
    stream = stream.lstrip("spead://")
    ip_range, port = stream.split(":")
    port = int(port)
    try:
        base_ip, ip_count = ip_range.split("+")
        ip_count = int(ip_count)
    except ValueError:
        base_ip, ip_count = ip_range, 1
    return ContiguousIpRange(base_ip, port, ip_count)

class IpRangeAllocationError(Exception):
    pass

class ContiguousIpRange(object):
    def __init__(self, base_ip, port, count):
        self._base_ip = ipaddress.ip_address(unicode(base_ip))
        self._ips = [self._base_ip+ii for ii in range(count)]
        self._port = port
        self._count = count

    @property
    def count(self):
        return self._count

    @property
    def port(self):
        return self._port

    @property
    def base_ip(self):
        return self._base_ip

    def index(self, ip):
        return self._ips.index(ip)

    def __hash__(self):
        return hash(self.format_katcp())

    def __iter__(self):
        return self._ips.__iter__()

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__, self.format_katcp())

    def format_katcp(self):
        return "spead://{}+{}:{}".format(str(self._base_ip), self._count, self._port)


class IpRangeManager(object):
    def __init__(self, ip_range):
        self._ip_range = ip_range
        self._allocated = [False for _ in ip_range]
        self._allocated_ranges = set()

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__, self._ip_range.format_katcp())

    def format_katcp(self):
        return self._ip_range.format_katcp()

    def _free_ranges(self):
        state_ranges = {True:[], False:[]}
        def find_state_range(idx, state):
            start_idx = idx
            while idx < len(self._allocated):
                if self._allocated[idx] == state:
                    idx+=1
                else:
                    state_ranges[state].append((start_idx, idx-start_idx))
                    return find_state_range(idx, not state)
            else:
                state_ranges[state].append((start_idx, idx-start_idx))
        find_state_range(0, self._allocated[0])
        return state_ranges[False]

    def allocate(self, n):
        ranges = self._free_ranges()
        best_fit = None
        for start,span in ranges:
            if span<n:
                continue
            elif best_fit is None:
                best_fit = (start, span)
            elif (span-n) < (best_fit[1]-n):
                best_fit = (start, span)
        if best_fit is None:
            raise IpRangeAllocationError("Could not allocate contiguous range of {} addresses".format(n))
        else:
            start,span = best_fit
            for ii in range(n):
                offset = start+ii
                self._allocated[offset] = True
            allocated_range = ContiguousIpRange(str(self._ip_range.base_ip + start), self._ip_range.port, n)
            self._allocated_ranges.add(allocated_range)
            return allocated_range

    def free(self, ip_range):
        self._allocated_ranges.remove(ip_range)
        for ip in ip_range:
            self._allocated[self._ip_range.index(ip)] = False


###################
# Custom exceptions
###################

class ServerAllocationError(Exception):
    pass

class ServerDeallocationError(Exception):
    pass

class ProductLookupError(Exception):
    pass

class ProductExistsError(Exception):
    pass

class AntennaValidationError(Exception):
    pass

class FbfStateError(Exception):
    def __init__(self, expected_states, current_state):
        message = "Possible states for this operation are '{}', but current state is '{}'".format(
            expected_states, current_state)
        super(FbfStateError, self).__init__(message)


class KatportalClientWrapper(object):
    def __init__(self, host, sub_nr=1):
        self._client = KATPortalClient('http://{host}/api/client/{sub_nr}'.format(
            host=host, sub_nr=sub_nr),
            on_update_callback=None, logger=logging.getLogger('katcp'))

    @coroutine
    def _query(self, component, sensor):
        sensor_name = yield self._client.sensor_subarray_lookup(
            component=component, sensor=sensor, return_katcp_name=False)
        sensor_sample = yield self._client.sensor_value(sensor_name,
            include_value_ts=False)
        raise Return(sensor_sample)

    @coroutine
    def get_observer_string(self, antenna):
        sensor_sample = yield self._query(antenna, "observer")
        raise Return(sensor_sample.value)

    @coroutine
    def get_antenna_feng_id_map(self, instrument_name, antennas):
        sensor_sample = yield self._query('cbf', '{}.input-labelling'.format(instrument_name))
        labels = eval(sensor_sample.value)
        mapping = {}
        for input_label, input_index, _, _ in labels:
            antenna_name = input_label.strip("vh").lower()
            if antenna_name.startswith("m") and antenna_name in antennas:
                mapping[antenna_name] = input_index//2
        print mapping
        raise Return(mapping)

    @coroutine
    def get_bandwidth(self, stream):
        sensor_sample = yield self._query('sub', 'streams.{}.bandwidth'.format(stream))
        raise Return(sensor_sample.value)

    @coroutine
    def get_cfreq(self, stream):
        sensor_sample = yield self._query('sub', 'streams.{}.centre-frequency'.format(stream))
        raise Return(sensor_sample.value)

    @coroutine
    def get_sideband(self, stream):
        sensor_sample = yield self._query('sub', 'streams.{}.sideband'.format(stream))
        raise Return(sensor_sample.value)

    @coroutine
    def gey_sync_epoch(self):
        sensor_sample = yield self._query('sub', 'synchronisation-epoch')
        raise Return(sensor_sample.value)

    @coroutine
    def get_itrf_reference(self):
        sensor_sample = yield self._query('sub', 'array-position-itrf')
        x, y, z = [float(i) for i in sensor_sample.value.split(",")]
        raise Return((x, y, z))



####################
# Classes for communicating with and wrapping
# the functionality of the processing servers
# on each NUMA node.
####################

class FbfWorkerWrapper(object):
    """Wrapper around a client to an FbfWorkerServer
    instance.
    """
    def __init__(self, hostname, port):
        """
        @brief  Create a new wrapper around a client to a worker server

        @params hostname The hostname for the worker server
        @params port     The port number that the worker server serves on
        """
        log.debug("Building client to FbfWorkerServer at {}:{}".format(hostname, port))
        self._client = KATCPClientResource(dict(
            name="worker-server-client",
            address=(hostname, port),
            controlled=True))
        self.hostname = hostname
        self.port = port
        self.priority = 0 # Currently no priority mechanism is implemented
        self._started = False

    def start(self):
        """
        @brief  Start the client to the worker server
        """
        log.debug("Starting client to FbfWorkerServer at {}:{}".format(self.hostname, self.port))
        self._client.start()
        self._started = True

    def __repr__(self):
        return "<{} for {}:{}>".format(self.__class__, self.hostname, self.port)

    def __hash__(self):
        # This has override is required to allow these wrappers
        # to be used with set() objects. The implication is that
        # the combination of hostname and port is unique for a
        # worker server
        return hash((self.hostname, self.port))

    def __eq__(self, other):
        # Also implemented to help with hashing
        # for sets
        return self.__hash__() == hash(other)

    def __del__(self):
        if self._started:
            try:
                self._client.stop()
            except Exception as error:
                log.exception(str(error))


class FbfWorkerPool(object):
    """Wrapper class for managing server
    allocation and deallocation to subarray/products
    """
    def __init__(self):
        """
        @brief   Construct a new instance
        """
        self._servers = set()
        self._allocated = set()

    def add(self, hostname, port):
        """
        @brief  Add a new FbfWorkerServer to the server pool

        @params hostname The hostname for the worker server
        @params port     The port number that the worker server serves on
        """
        wrapper = FbfWorkerWrapper(hostname,port)
        if not wrapper in self._servers:
            wrapper.start()
            log.debug("Adding {} to server set".format(wrapper))
            self._servers.add(wrapper)

    def remove(self, hostname, port):
        """
        @brief  Add a new FbfWorkerServer to the server pool

        @params hostname The hostname for the worker server
        @params port     The port number that the worker server serves on
        """
        wrapper = FbfWorkerWrapper(hostname,port)
        if wrapper in self._allocated:
            raise ServerDeallocationError("Cannot remove allocated server from pool")
        try:
            self._servers.remove(wrapper)
        except KeyError:
            log.warning("Could not find {}:{} in server pool".format(hostname, port))

    def allocate(self, count):
        """
        @brief    Allocate a number of servers from the pool.

        @note     Free servers will be allocated by priority order
                  with 0 being highest priority

        @return   A list of FbfWorkerWrapper objects
        """
        with lock:
            log.debug("Request to allocate {} servers".format(count))
            available_servers = list(self._servers.difference(self._allocated))
            log.debug("{} servers available".format(len(available_servers)))
            available_servers.sort(key=lambda server: server.priority, reverse=True)
            if len(available_servers) < count:
                raise ServerAllocationError("Cannot allocate {0} servers, only {1} available".format(
                    count, len(available_servers)))
            allocated_servers = []
            for _ in range(count):
                server = available_servers.pop()
                log.debug("Allocating server: {}".format(server))
                allocated_servers.append(server)
                self._allocated.add(server)
            return allocated_servers

    def deallocate(self, servers):
        """
        @brief    Deallocate servers and return the to the pool.

        @param    A list of Node objects
        """
        for server in servers:
            log.debug("Deallocating server: {}".format(server))
            self._allocated.remove(server)

    def reset(self):
        """
        @brief   Deallocate all servers
        """
        log.debug("Reseting server pool allocations")
        self._allocated = set()

    def available(self):
        """
        @brief   Return list of available servers
        """
        return list(self._servers.difference(self._allocated))

    def used(self):
        """
        @brief   Return list of allocated servers
        """
        return list(self._allocated)

####################
# The main CAM interface for FBFUSE
####################

class FbfMasterController(AsyncDeviceServer):
    """This is the main KATCP interface for the FBFUSE
    multi-beam beamformer on MeerKAT.

    This interface satisfies the following ICDs:
    CAM-FBFUSE: <link>
    TUSE-FBFUSE: <link>
    """
    VERSION_INFO = ("mpikat-fbf-api", 0, 1)
    BUILD_INFO = ("mpikat-fbf-implementation", 0, 1, "rc1")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]
    def __init__(self, ip, port, dummy=True,
        ip_range = FBF_IP_RANGE):
        """
        @brief       Construct new FbfMasterController instance

        @params  ip       The IP address on which the server should listen
        @params  port     The port that the server should bind to
        @params  dummy    Specifies if the instance is running in a dummy mode

        @note   In dummy mode, the controller will act as a mock interface only, sending no requests to nodes.
                A valid node pool must still be provided to the instance, but this may point to non-existent nodes.

        """
        self._ip_pool = IpRangeManager(ip_range_from_stream(ip_range))
        super(FbfMasterController, self).__init__(ip,port)
        self._products = {}
        self._dummy = dummy
        self._server_pool = FbfWorkerPool()

    def start(self):
        """
        @brief  Start the FbfMasterController server
        """
        super(FbfMasterController,self).start()

    def setup_sensors(self):
        """
        @brief  Set up monitoring sensors.

        @note   The following sensors are made available on top of default sensors
                implemented in AsynDeviceServer and its base classes.

                device-status:  Reports the health status of the FBFUSE and associated devices:
                                Among other things report HW failure, SW failure and observation failure.

                local-time-synced:  Indicates whether the local time of FBFUSE servers
                                    is synchronised to the master time reference (use NTP).
                                    This sensor is aggregated from all nodes that are part
                                    of FBF and will return "not sync'd" if any nodes are
                                    unsyncronised.

                products:   The list of product_ids that FBFUSE is currently handling
        """
        self._device_status = Sensor.discrete(
            "device-status",
            description="Health status of FBFUSE",
            params=self.DEVICE_STATUSES,
            default="ok",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._device_status)

        self._local_time_synced = Sensor.boolean(
            "local-time-synced",
            description="Indicates FBF is NTP syncronised.",
            default=True,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._local_time_synced)

        self._products_sensor = Sensor.string(
            "products",
            description="The names of the currently configured products",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._products_sensor)

        self._ip_pool_sensor = Sensor.string(
            "output-ip-range",
            description="The multicast address allocation for coherent beams",
            default=self._ip_pool.format_katcp(),
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._ip_pool_sensor)



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
        @brief   Register an FbfWorker instance

        @params hostname The hostname for the worker server
        @params port     The port number that the worker server serves on

        @detail  Register an FbfWorker instance that can be used for FBFUSE
                 computation. FBFUSE has no preference for the order in which control
                 servers are allocated to a subarray. An FbfWorker wraps an atomic
                 unit of compute comprised of one CPU, one GPU and one NIC (i.e. one NUMA
                 node on an FBFUSE compute server).
        """
        self._server_pool.add(hostname, port)
        return ("ok",)

    @request(Str(), Int())
    @return_reply()
    def request_deregister_worker_server(self, req, hostname, port):
        """
        @brief   Deregister an FbfWorker instance

        @params hostname The hostname for the worker server
        @params port     The port number that the worker server serves on

        @detail  The graceful way of removing a server from rotation. If the server is
                 currently actively processing an exception will be raised.
        """
        try:
            self._server_pool.remove(hostname, port)
        except ServerDeallocationError as error:
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


    @request(Str(), Str(), Int(), Str(), Str())
    @return_reply()
    def request_configure(self, req, product_id, antennas_csv, n_channels, streams_json, proxy_name):
        """
        @brief      Configure FBFUSE to receive and process data from a subarray

        @detail     REQUEST ?configure product_id antennas_csv n_channels streams_json proxy_name
                    Configure FBFUSE for the particular data products

        @param      req               A katcp request object

        @param      product_id        This is a name for the data product, which is a useful tag to include
                                      in the data, but should not be analysed further. For example "array_1_bc856M4k".

        @param      antennas_csv      A comma separated list of physical antenna names used in particular sub-array
                                      to which the data products belongs (e.g. m007,m008,m009).

        @param      n_channels        The integer number of frequency channels provided by the CBF.

        @param      streams_json      a JSON struct containing config keys and values describing the streams.

                                      For example:

                                      @code
                                         {'stream_type1': {
                                             'stream_name1': 'stream_address1',
                                             'stream_name2': 'stream_address2',
                                             ...},
                                             'stream_type2': {
                                             'stream_name1': 'stream_address1',
                                             'stream_name2': 'stream_address2',
                                             ...},
                                          ...}
                                      @endcode

                                      The steam type keys indicate the source of the data and the type, e.g. cam.http.
                                      stream_address will be a URI.  For SPEAD streams, the format will be spead://<ip>[+<count>]:<port>,
                                      representing SPEAD stream multicast groups. When a single logical stream requires too much bandwidth
                                      to accommodate as a single multicast group, the count parameter indicates the number of additional
                                      consecutively numbered multicast group ip addresses, and sharing the same UDP port number.
                                      stream_name is the name used to identify the stream in CAM.
                                      A Python example is shown below, for five streams:
                                      One CAM stream, with type cam.http.  The camdata stream provides the connection string for katportalclient
                                      (for the subarray that this FBFUSE instance is being configured on).
                                      One F-engine stream, with type:  cbf.antenna_channelised_voltage.
                                      One X-engine stream, with type:  cbf.baseline_correlation_products.
                                      Two beam streams, with type: cbf.tied_array_channelised_voltage.  The stream names ending in x are
                                      horizontally polarised, and those ending in y are vertically polarised.

                                      @code
                                         pprint(streams_dict)
                                         {'cam.http':
                                             {'camdata':'http://10.8.67.235/api/client/1'},
                                          'cbf.antenna_channelised_voltage':
                                             {'i0.antenna-channelised-voltage':'spead://239.2.1.150+15:7148'},
                                          ...}
                                      @endcode

                                      If using katportalclient to get information from CAM, then reconnect and re-subscribe to all sensors
                                      of interest at this time.

        @param      proxy_name        The CAM name for the instance of the FBFUSE data proxy that is being configured.
                                      For example, "FBFUSE_3".  This can be used to query sensors on the correct proxy,
                                      in the event that there are multiple instances in the same subarray.

        @note       A configure call will result in the generation of a new subarray instance in FBFUSE that will be added to the clients list.

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """
        # Test if product_id already exists
        if product_id in self._products:
            return ("fail", "FBF already has a configured product with ID: {}".format(product_id))
        # Determine number of nodes required based on number of antennas in subarray
        # Note this is a poor way of handling this that may be updated later. In theory
        # there is a throughput measure as a function of bandwidth, polarisations and number
        # of antennas that allows one to determine the number of nodes to run. Currently we
        # just assume one antennas worth of data per NIC on our servers, so two antennas per
        # node.
        try:
            antennas = parse_csv_antennas(antennas_csv)
        except AntennaValidationError as error:
            return ("fail", str(error))

        valid_n_channels = [1024, 4096, 32768]
        if not n_channels in valid_n_channels:
            return ("fail", "The provided number of channels ({}) is not valid. Valid options are {}".format(n_channels, valid_n_channels))

        streams = json.loads(streams_json)
        try:
            streams['cam.http']['camdata']
            # Need to check for endswith('.antenna-channelised-voltage') as the i0 is not
            # guaranteed to stay the same.
            # i0 = instrument name
            # Need to keep this for future sensor lookups
            streams['cbf.antenna_channelised_voltage']
        except KeyError as error:
            return ("fail", "JSON streams object does not contain required key: {}".format(str(error)))

        for key in streams['cbf.antenna_channelised_voltage'].keys():
            if key.endswith('.antenna-channelised-voltage'):
                instrument_name, _ = key.split('.')
                feng_stream_name = key
                break
        else:
            return ("fail", "Could not determine instrument name (e.g. 'i0') from streams")

        # TODO: change this request to @async_reply and make the whole thing a coroutine
        @coroutine
        def configure():
            nantennas = len(antennas)
            if not is_power_of_two(nantennas):
                log.warning("Number of antennas was not a power of two. Rounding up to next power of two for resource allocation.")
            if next_power_of_two(nantennas)//2 < 4:
                log.warning("Number of antennas was less than than 4 but resources will be allocated assuming 4 antennas.")
            required_servers = max(4, next_power_of_two(nantennas)//2)

            # Want to make all the katportalclient calls here, retrieving:
            # - observer strings
            # - reference position
            # - bandwidth
            # - centre frequency
            # - sideband

            kpc = KatportalClientWrapper(PORTAL)

            # Get all antenna observer strings
            futures, observers = [],[]
            for antenna in antennas:
                log.debug("Fetching katpoint string for antenna {}".format(antenna))
                futures.append(kpc.get_observer_string(antenna))
            for ii,future in enumerate(futures):
                try:
                    observer = yield future
                except Exception as error:
                    log.error("Error on katportalclient call: {}".format(str(error)))
                    req.reply("fail", "Error retrieving katpoint string for antenna {}".format(antennas[ii]))
                    return
                else:
                    log.debug("Fetched katpoint antenna: {}".format(observer))
                    observers.append(Antenna(observer))

            # Get bandwidth, cfreq, sideband, f-eng mapping
            bandwidth_future = kpc.get_bandwidth(feng_stream_name)
            cfreq_future = kpc.get_cfreq(feng_stream_name)
            sideband_future = kpc.get_sideband(feng_stream_name)
            feng_antenna_map_future = kpc.get_antenna_feng_id_map(instrument_name, antennas)
            bandwidth = yield bandwidth_future
            cfreq = yield cfreq_future
            sideband = yield sideband_future
            feng_antenna_map = yield feng_antenna_map_future


            # This may be removed in future.
            # Currently if self._dummy is set no actual server allocation will be requested.
            if not self._dummy:
                servers = self._server_pool.allocate(required_servers)
            else:
                servers = []
            product = FbfProductController(self, product_id, observers, n_channels, streams, proxy_name, servers)
            self._products[product_id] = product
            self._update_products_sensor()
            req.reply("ok",)
        self.ioloop.add_callback(configure)
        raise AsyncReply

    @request(Str())
    @return_reply()
    def request_deconfigure(self, req, product_id):
        """
        @brief      Deconfigure the FBFUSE instance.

        @note       Deconfigure the FBFUSE instance. If FBFUSE uses katportalclient to get information
                    from CAM, then it should disconnect at this time.

        @param      req               A katcp request object

        @param      product_id        This is a name for the data product, used to track which subarray is being deconfigured.
                                      For example "array_1_bc856M4k".

        @return     katcp reply object [[[ !deconfigure ok | (fail [error description]) ]]]
        """
        # Test if product exists
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            return ("fail", str(error))
        try:
            product.stop_beams()
        except Exception as error:
            return ("fail", str(error))
        self._server_pool.deallocate(product.servers)
        product.teardown_sensors()
        del self._products[product_id]
        self._update_products_sensor()
        return ("ok",)


    @request(Str(), Str())
    @return_reply()
    @coroutine
    def request_target_start(self, req, product_id, target):
        """
        @brief      Notify FBFUSE that a new target is being observed

        @param      product_id      This is a name for the data product, used to track which subarray is being deconfigured.
                                    For example "array_1_bc856M4k".

        @param      target          A KATPOINT target string

        @return     katcp reply object [[[ !target-start ok | (fail [error description]) ]]]
        """
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            raise Return(("fail", str(error)))
        try:
            target = Target(target)
        except Exception as error:
            raise Return(("fail", str(error)))
        yield product.target_start(target)
        raise Return(("ok",))


    # DELETE this

    @request(Str())
    @return_reply()
    @coroutine
    def request_target_stop(self, req, product_id):
        """
        @brief      Notify FBFUSE that the telescope has stopped observing a target

        @param      product_id      This is a name for the data product, used to track which subarray is being deconfigured.
                                    For example "array_1_bc856M4k".

        @return     katcp reply object [[[ !target-start ok | (fail [error description]) ]]]
        """
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            raise Return(("fail", str(error)))
        yield product.target_stop()
        raise Return(("ok",))


    @request(Str(), Int(), Str(), Int(), Int())
    @return_reply()
    def request_configure_coherent_beams(self, req, product_id, nbeams, antennas_csv, fscrunch, tscrunch):
        """
        @brief      Request that FBFUSE configure parameters for coherent beams

        @note       This call can only be made prior to a call to start-beams for the configured product.
                    This is due to FBFUSE requiring static information up front in order to compile beamformer
                    kernels, allocate the correct size memory buffers and subscribe to the correct number of
                    multicast groups.

        @note       The particular configuration passed at this stage will only be evaluated on a call to start-beams.
                    If the requested configuration is not possible due to hardware and bandwidth limits and error will
                    be raised on the start-beams call.

        @param      req             A katcp request object

        @param      product_id      This is a name for the data product, used to track which subarray is being deconfigured.
                                    For example "array_1_bc856M4k".

        @param      nbeams          The number of beams that will be produced for the provided product_id

        @param      antennas_csv    A comma separated list of physical antenna names. Only these antennas will be used
                                    when generating coherent beams (e.g. m007,m008,m009). The antennas provided here must
                                    be a subset of the antennas in the current subarray. If not an exception will be
                                    raised.

        @param      fscrunch        The number of frequency channels to integrate over when producing coherent beams.

        @param      tscrunch        The number of time samples to integrate over when producing coherent beams.

        @return     katcp reply object [[[ !configure-coherent-beams ok | (fail [error description]) ]]]
        """
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            return ("fail", str(error))
        try:
            product.configure_coherent_beams(nbeams, antennas_csv, fscrunch, tscrunch)
        except Exception as error:
            return ("fail", str(error))
        else:
            return ("ok",)

    @request(Str(), Str(), Int(), Int())
    @return_reply()
    def request_configure_incoherent_beam(self, req, product_id, antennas_csv, fscrunch, tscrunch):
        """
        @brief      Request that FBFUSE sets the parameters for the incoherent beam

        @note       The particular configuration passed at this stage will only be evaluated on a call to start-beams.
                    If the requested configuration is not possible due to hardware and bandwidth limits and error will
                    be raised on the start-beams call.

        @note       Currently FBFUSE is only set to produce one incoherent beam per instantiation. This may change in future.

        @param      req             A katcp request object

        @param      product_id      This is a name for the data product, used to track which subarray is being deconfigured.
                                    For example "array_1_bc856M4k".

        @param      nbeams          The number of beams that will be produced for the provided product_id

        @param      antennas_csv    A comma separated list of physical antenna names. Only these antennas will be used
                                    when generating the incoherent beam (e.g. m007,m008,m009). The antennas provided here must
                                    be a subset of the antennas in the current subarray. If not an exception will be
                                    raised.

        @param      fscrunch        The number of frequency channels to integrate over when producing the incoherent beam.

        @param      tscrunch        The number of time samples to integrate over when producing the incoherent beam.

        @return     katcp reply object [[[ !configure-incoherent-beam ok | (fail [error description]) ]]]
        """
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            return ("fail", str(error))
        try:
            product.configure_incoherent_beam(antennas_csv, fscrunch, tscrunch)
        except Exception as error:
            return ("fail", str(error))
        else:
            return ("ok",)

    @request(Str())
    @return_reply()
    def request_capture_start(self, req, product_id):
        """
        @brief      Request that FBFUSE start beams streaming

        @detail     Upon this call the provided coherent and incoherent beam configurations will be evaluated
                    to determine if they are physical and can be met with the existing hardware. If the configurations
                    are acceptable then servers allocated to this instance will be triggered to begin production of beams.

        @param      req               A katcp request object

        @param      product_id        This is a name for the data product, used to track which subarray is being deconfigured.
                                      For example "array_1_bc856M4k".

        @return     katcp reply object [[[ !start-beams ok | (fail [error description]) ]]]
        """
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            return ("fail", str(error))
        @coroutine
        def start():
            try:
                product.start_capture()
            except Exception as error:
                req.reply("fail", str(error))
            else:
                req.reply("ok",)
        self.ioloop.add_callback(start)
        raise AsyncReply

    @request(Str())
    @return_reply()
    def request_provision_beams(self, req, product_id):
        """
        @brief      Request that FBFUSE asynchronously prepare to start beams streaming

        @detail     Upon this call the provided coherent and incoherent beam configurations will be evaluated
                    to determine if they are physical and can be met with the existing hardware. If the configurations
                    are acceptable then servers allocated to this instance will be triggered to prepare for the production of beams.
                    Unlike a call to ?capture-start, ?provision-beams will not trigger a connection to multicast groups and will not
                    wait for completion before returning, instead it will start the process of beamformer resource alloction and compilation.
                    To determine when the process is complete, the user must wait on the value of the product "state" sensor becoming "ready",
                    e.g.

                    @code
                        client.sensor['{}-state'.format(proxy_name)].wait(
                            lambda reading: reading.value == 'ready')
                    @endcode

        @param      req               A katcp request object

        @param      product_id        This is a name for the data product, used to track which subarray is being deconfigured.
                                      For example "array_1_bc856M4k".

        @return     katcp reply object [[[ !start-beams ok | (fail [error description]) ]]]
        """
        # Note: the state of the product won't be updated until the start call hits the top of the
        # event loop. It may be preferable to keep a self.starting_future object and yield on it
        # in capture-start if it exists. The current implementation may or may not be a bug...
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            return ("fail", str(error))
        # This check needs to happen here as this call
        # should return immediately
        if not product.idle:
            return ("fail", "Can only provision beams on an idle FBF product")
        self.ioloop.add_callback(product.prepare)
        return ("ok",)

    @request(Str())
    @return_reply()
    def request_capture_stop(self, req, product_id):
        """
        @brief      Stop FBFUSE streaming

        @param      product_id      This is a name for the data product, used to track which subarray is being deconfigured.
                                    For example "array_1_bc856M4k".
        """
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            return ("fail", str(error))
        @coroutine
        def stop():
            product.stop_beams()
            req.reply("ok",)
        self.ioloop.add_callback(stop)
        raise AsyncReply

    @request(Str(), Str(), Int())
    @return_reply()
    def request_set_configuration_authority(self, req, product_id, hostname, port):
        """
        @brief     Set the configuration authority for an FBF product

        @detail    The parameters passed here specify the address of a server that
                   can be triggered to provide FBFUSE with configuration information
                   at schedule block and target boundaries. The configuration authority
                   must be a valid KATCP server.
        """
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            return ("fail", str(error))
        product.set_configuration_authority(hostname, port)
        return ("ok",)

    @request(Str())
    @return_reply()
    def request_reset_beams(self, req, product_id):
        """
        @brief      Reset the positions of all allocated beams

        @note       This call may only be made AFTER a successful call to start-beams. Before this point no beams are
                    allocated to the instance. If all beams are currently allocated an exception will be raised.

        @param      req             A katcp request object

        @param      product_id      This is a name for the data product, used to track which subarray is being deconfigured.
                                    For example "array_1_bc856M4k".

        @return     katcp reply object [[[ !reset-beams m ok | (fail [error description]) ]]]
        """
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            return ("fail", str(error))
        else:
            beam = product.reset_beams()
            return ("ok", )

    @request(Str(), Str())
    @return_reply(Str())
    def request_add_beam(self, req, product_id, target):
        """
        @brief      Configure the parameters of one beam

        @note       This call may only be made AFTER a successful call to start-beams. Before this point no beams are
                    allocated to the instance. If all beams are currently allocated an exception will be raised.

        @param      req             A katcp request object

        @param      product_id      This is a name for the data product, used to track which subarray is being deconfigured.
                                    For example "array_1_bc856M4k".

        @param      target          A KATPOINT target string

        @return     katcp reply object [[[ !add-beam ok | (fail [error description]) ]]]
        """
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            return ("fail", str(error))
        try:
            target = Target(target)
        except Exception as error:
            return ("fail", str(error))
        beam = product.add_beam(target)
        return ("ok", beam.idx)

    @request(Str(), Str(), Int(), Float(), Float(), Float())
    @return_reply(Str())
    def request_add_tiling(self, req, product_id, target, nbeams, reference_frequency, overlap, epoch):
        """
        @brief      Configure the parameters of a static beam tiling

        @note       This call may only be made AFTER a successful call to start-beams. Before this point no beams are
                    allocated to the instance. If there are not enough free beams to satisfy the request an
                    exception will be raised.

        @note       Beam shapes calculated for tiling are always assumed to be 2D elliptical Gaussians.

        @param      req             A katcp request object

        @param      product_id      This is a name for the data product, used to track which subarray is being deconfigured.
                                    For example "array_1_bc856M4k".

        @param      target          A KATPOINT target string

        @param      nbeams          The number of beams in this tiling pattern.

        @param      reference_frequency     The reference frequency at which to calculate the synthesised beam shape,
                                            and thus the tiling pattern. Typically this would be chosen to be the
                                            centre frequency of the current observation.

        @param      overlap         The desired overlap point between beams in the pattern. The overlap defines
                                    at what power point neighbouring beams in the tiling pattern will meet. For
                                    example an overlap point of 0.1 corresponds to beams overlapping only at their
                                    10%-power points. Similarly a overlap of 0.5 corresponds to beams overlapping
                                    at their half-power points. [Note: This is currently a tricky parameter to use
                                    when values are close to zero. In future this may be define in sigma units or
                                    in multiples of the FWHM of the beam.]

        @param      epoch           The desired epoch for the tiling pattern as a unix time. A typical usage would
                                    be to set the epoch to half way into the coming observation in order to minimise
                                    the effect of parallactic angle and array projection changes altering the shape
                                    and position of the beams and thus changing the efficiency of the tiling pattern.


        @return     katcp reply object [[[ !add-tiling ok | (fail [error description]) ]]]
        """
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            return ("fail", str(error))
        try:
            target = Target(target)
        except Exception as error:
            return ("fail", str(error))
        tiling = product.add_tiling(target, nbeams, reference_frequency, overlap, epoch)
        return ("ok", tiling.idxs())

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

    @request(Str(), Str())
    @return_reply()
    def request_set_default_target_configuration(self, req, product_id, target):
        """
        @brief      Set the configuration of FBFUSE from the FBFUSE configuration server

        @param      product_id      This is a name for the data product, used to track which subarray is being deconfigured.
                                    For example "array_1_bc856M4k".

        @param      target          A KATPOINT target string
        """
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            return ("fail", str(error))
        try:
            target = Target(target)
        except Exception as error:
            return ("fail", str(error))
        if not product.capturing:
            return ("fail","Product must be capturing before a target confiugration can be set.")
        product.reset_beams()
        # TBD: Here we connect to some database and request the default configurations
        # For example this may return secondary target in the FoV
        #
        # As a default the current system will put one beam directly on target and
        # the rest of the beams in a static tiling pattern around this target
        now = time.time()
        nbeams = product._beam_manager.nbeams
        product.add_tiling(target, nbeams-1, 1.4e9, 0.5, now)
        product.add_beam(target)
        return ("ok",)

    @request(Str(), Str())
    @return_reply()
    def request_set_default_sb_configuration(self, req, product_id, sb_id):
        """
        @brief      Set the configuration of FBFUSE from the FBFUSE configuration server

        @param      product_id      This is a name for the data product, used to track which subarray is being deconfigured.
                                    For example "array_1_bc856M4k".

        @param      sb_id           The schedule block ID. Decisions of the configuarion of FBFUSE will be made dependent on
                                    the configuration of the current subarray, the primary and secondary science projects
                                    active and the targets expected to be visted during the execution of the schedule block.
        """
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            return ("fail", str(error))
        if product.capturing:
            return ("fail", "Cannot reconfigure a currently capturing instance.")
        product.configure_coherent_beams(400, product._katpoint_antennas, 1, 16)
        product.configure_incoherent_beam(product._katpoint_antennas, 1, 16)
        now = time.time()
        nbeams = product._beam_manager.nbeams
        product.add_tiling(target, nbeams-1, 1.4e9, 0.5, now)
        product.add_beam(target)
        return ("ok",)

####################
# Classes representing the different
# beams and tilings that can be provided
# by FBFUSE
####################

DEFAULT_KATPOINT_TARGET = "unset, radec, 0, 0"

class Beam(object):
    """Wrapper class for a single beam to be produced
    by FBFUSE"""
    def __init__(self, idx, target=DEFAULT_KATPOINT_TARGET):
        """
        @brief   Create a new Beam object

        @params   idx   a unique identifier for this beam.

        @param      target          A KATPOINT target object
        """
        self.idx = idx
        self._target = target
        self._observers = set()

    @property
    def target(self):
        return self._target

    @target.setter
    def target(self, new_target):
        self._target = new_target
        self.notify()

    def notify(self):
        """
        @brief  Notify all observers of a change to the beam parameters
        """
        for observer in self._observers:
            observer(self)

    def register_observer(self, func):
        """
        @brief   Register an observer to be called on a notify

        @params  func  Any function that takes a Beam object as its only argument
        """
        self._observers.add(func)

    def deregister_observer(self, func):
        """
        @brief   Deregister an observer to be called on a notify

        @params  func  Any function that takes a Beam object as its only argument
        """
        self._observers.remove(func)

    def reset(self):
        """
        @brief   Reset the beam to default parameters
        """
        self.target = Target(DEFAULT_KATPOINT_TARGET)

    def __repr__(self):
        return "{}, {}".format(
            self.idx, self.target.format_katcp())


class Tiling(object):
    """Wrapper class for a collection of beams in a tiling pattern
    """
    def __init__(self, target, reference_frequency, overlap):
        """
        @brief   Create a new tiling object

        @param      target          A KATPOINT target object

        @param      reference_frequency     The reference frequency at which to calculate the synthesised beam shape,
                                            and thus the tiling pattern. Typically this would be chosen to be the
                                            centre frequency of the current observation.

        @param      overlap         The desired overlap point between beams in the pattern. The overlap defines
                                    at what power point neighbouring beams in the tiling pattern will meet. For
                                    example an overlap point of 0.1 corresponds to beams overlapping only at their
                                    10%-power points. Similarly a overlap of 0.5 corresponds to beams overlapping
                                    at their half-power points. [Note: This is currently a tricky parameter to use
                                    when values are close to zero. In future this may be define in sigma units or
                                    in multiples of the FWHM of the beam.]
        """
        self._beams = []
        self.target = target
        self.reference_frequency = reference_frequency
        self.overlap = overlap
        self.tiling = None

    @property
    def nbeams(self):
        return len(self._beams)

    def add_beam(self, beam):
        """
        @brief   Add a beam to the tiling pattern

        @param   beam   A Beam object
        """
        self._beams.append(beam)

    def generate(self, antennas, epoch):
        """
        @brief   Calculate and update RA and Dec positions of all
                 beams in the tiling object.

        @param      epoch     The epoch of tiling (unix time)

        @param      antennas  The antennas to use when calculating the beam shape.
                              Note these are the antennas in katpoint CSV format.
        """
        psfsim = mosaic.PsfSim(antennas, self.reference_frequency)
        beam_shape = psfsim.get_beam_shape(self.target, epoch)
        tiling = mosaic.generate_nbeams_tiling(beam_shape, self.nbeams, self.overlap)
        for ii in range(tiling.beam_num):
            ra, dec = tiling.coordinates[ii]
            self._beams[ii].target = Target('{},radec,{},{}'.format(self.target.name, ra, dec))
        log.warning("Current mosaic implementation returns incorrect tiling positions")

    def __repr__(self):
        return ", ".join([repr(beam) for beam in self._beams])

    def idxs(self):
        return ",".join([beam.idx for beam in self._beams])



class BeamManager(object):
    """Manager class for allocation, deallocation and tracking of
    individual beams and static tilings.
    """
    def __init__(self, nbeams, antennas):
        """
        @brief  Create a new beam manager object

        @param  nbeams    The number of beams managed by this object

        @param  antennas  A list of antennas to use for tilings. Note these should
                          be in KATPOINT CSV format.
        """
        self._nbeams = nbeams
        self._antennas = antennas
        self._beams = [Beam("cfbf%05d"%(i)) for i in range(self._nbeams)]
        self._free_beams = [beam for beam in self._beams]
        self._allocated_beams = []

        self.reset()

    @property
    def nbeams(self):
        return self._nbeams

    @property
    def antennas(self):
        return self._antennas

    def reset(self):
        """
        @brief  reset and deallocate all beams and tilings managed by this instance

        @note   All tiling will be lost on this call and must be remade for subsequent observations
        """
        for beam in self._beams:
            beam.reset()
        self._free_beams = [beam for beam in self._beams]
        self._allocated_beams = []
        self._tilings = []
        self._dynamic_tilings = []

    def __add_beam(self, target):
        beam = self._free_beams.pop(0)
        beam.target = target
        self._allocated_beams.append(beam)
        return beam

    def add_beam(self, target):
        """
        @brief   Specify the parameters of one managed beam

        @param      target          A KATPOINT target object

        @return     Returns the allocated Beam object
        """
        beam = self.__add_beam(target)
        return beam

    def __make_tiling(self, nbeams, tiling_type, *args):
        if len(self._free_beams) < nbeams:
            raise Exception("More beams requested than are available.")
        tiling = tiling_type(*args)
        for _ in range(nbeams):
            beam = self._free_beams.pop(0)
            tiling.add_beam(beam)
            self._allocated_beams.append(beam)
        return tiling

    def add_tiling(self, target, nbeams, reference_frequency, overlap):
        """
        @brief   Add a tiling to be managed

        @param      target          A KATPOINT target object

        @param      reference_frequency     The reference frequency at which to calculate the synthesised beam shape,
                                            and thus the tiling pattern. Typically this would be chosen to be the
                                            centre frequency of the current observation.

        @param      overlap         The desired overlap point between beams in the pattern. The overlap defines
                                    at what power point neighbouring beams in the tiling pattern will meet. For
                                    example an overlap point of 0.1 corresponds to beams overlapping only at their
                                    10%-power points. Similarly a overlap of 0.5 corresponds to beams overlapping
                                    at their half-power points. [Note: This is currently a tricky parameter to use
                                    when values are close to zero. In future this may be define in sigma units or
                                    in multiples of the FWHM of the beam.]

        @returns    The created Tiling object
        """
        if len(self._free_beams) < nbeams:
            raise Exception("More beams requested than are available.")
        tiling = Tiling(target, reference_frequency, overlap)
        for _ in range(nbeams):
            beam = self._free_beams.pop(0)
            tiling.add_beam(beam)
            self._allocated_beams.append(beam)
        self._tilings.append(tiling)
        return tiling

    def get_beams(self):
        """
        @brief  Return all managed beams
        """
        return self._allocated_beams + self._free_beams


####################
# Classes for wrapping an individual
# configuration of FBFUSE (i.e. an FBF product)
####################

class DelayEngine(AsyncDeviceServer):
    """A server for maintining delay models used
    by FbfWorkerServers.
    """
    VERSION_INFO = ("delay-engine-api", 0, 1)
    BUILD_INFO = ("delay-engine-implementation", 0, 1, "rc1")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]

    def __init__(self, ip, port, beam_manager):
        """
        @brief  Create a new DelayEngine instance

        @param   ip   The interface that the DelayEngine should serve on

        @param   port The port that the DelayEngine should serve on

        @param   beam_manager  A BeamManager instance that will be used to create delays
        """
        self._beam_manager = beam_manager
        super(DelayEngine, self).__init__(ip,port)

    def setup_sensors(self):
        """
        @brief    Set up monitoring sensors.

        @note     The key sensor here is the delay sensor which is stored in JSON format

                  @code
                  {
                  'antennas':['m007','m008','m009'],
                  'beams':['cfbf00001','cfbf00002'],
                  'model': [[[0,2],[0,5]],[[2,3],[4,4]],[[8,8],[8,8]]]
                  }
                  @endcode

                  Here the delay model is stored as a 3 dimensional array
                  with dimensions of beam, antenna, model (rate,offset) from
                  outer to inner dimension.
        """
        self._update_rate_sensor = Sensor.float(
            "update-rate",
            description="The delay update rate",
            default=2.0,
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._update_rate_sensor)

        self._nbeams_sensor = Sensor.integer(
            "nbeams",
            description="Number of beams that this delay engine handles",
            default=0,
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._nbeams_sensor)

        self._antennas_sensor = Sensor.string(
            "antennas",
            description="JSON breakdown of the antennas (in KATPOINT format) associated with this delay engine",
            default=json.dumps([a.format_katcp() for a in self._beam_manager.antennas]),
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._antennas_sensor)

        self._delays_sensor = Sensor.string(
            "delays",
            description="JSON object containing delays for each beam for each antenna at the current epoch",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.update_delays()
        self.add_sensor(self._delays_sensor)

    def update_delays(self):
        reference_antenna = Antenna("reference,{ref.lat},{ref.lon},{ref.elev}".format(
            ref=self._beam_manager.antennas[0].ref_observer))
        targets = [beam.target for beam in self._beam_manager.get_beams()]
        delay_calc = mosaic.DelayPolynomial(self._beam_manager.antennas, targets, reference_antenna)
        poly = delay_calc.get_delay_polynomials(time.time(), duration=self._update_rate_sensor.value()*2)
        #poly has format: beam, antenna, (delay, rate)
        output = {}
        output["beams"] = [beam.idx for beam in self._beam_manager.get_beams()]
        output["antennas"] = [ant.name for ant in self._beam_manager.antennas]
        output["model"] = poly.tolist()
        self._delays_sensor.set_value(json.dumps(output))

    def start(self):
        super(DelayEngine, self).start()


    @request(Float())
    @return_reply()
    def request_set_update_rate(self, req, rate):
        """
        @brief    Set the update rate for delay calculations

        @param    rate  The update rate for recalculation of delay polynomials
        """
        self._update_rate_sensor.set_value(rate)
        # This should make a change to the beam manager object

        self.update_delays()
        return ("ok",)


class FbfProductController(object):
    """
    Wrapper class for an FBFUSE product.
    """
    STATES = ["idle", "preparing", "ready", "starting", "capturing", "stopping"]
    IDLE, PREPARING, READY, STARTING, CAPTURING, STOPPING = STATES

    def __init__(self, parent, product_id, katpoint_antennas, n_channels, streams, proxy_name, servers):
        """
        @brief      Construct new instance

        @param      parent            The parent FbfMasterController instance

        @param      product_id        The name of the product

        @param      katpoint_antennas A list of katpoint.Antenna objects

        @param      n_channels        The integer number of frequency channels provided by the CBF.

        @param      streams           A dictionary containing config keys and values describing the streams.

        @param      proxy_name        The name of the proxy associated with this subarray (used as a sensor prefix)

        @param      servers           A list of FbfWorkerServer instances allocated to this product controller
        """
        log.debug("Creating new FbfProductController with args: {}".format(
            ", ".join([str(i) for i in (parent, product_id, katpoint_antennas, n_channels,
                streams, proxy_name, servers)])))
        self._parent = parent
        self._product_id = product_id
        self._antennas = ",".join([a.name for a in katpoint_antennas])
        self._katpoint_antennas = katpoint_antennas
        self._antenna_map = {a.name: a for a in self._katpoint_antennas}
        self._n_channels = n_channels
        self._streams = streams
        self._proxy_name = proxy_name
        self._servers = servers
        self._beam_manager = None
        self._delay_engine = None
        self._coherent_beam_ip_range = None
        self._ca_client = None
        self._managed_sensors = []
        self.setup_sensors()

    def __del__(self):
        self.teardown_sensors()

    def info(self):
        """
        @brief    Return a metadata dictionary describing this product controller
        """
        out = {
            "antennas":self._antennas,
            "nservers":len(self.servers),
            "capturing":self.capturing,
            "streams":self._streams,
            "nchannels":self._n_channels,
            "proxy_name":self._proxy_name
        }
        return out

    def add_sensor(self, sensor):
        """
        @brief    Add a sensor to the parent object

        @note     This method is used to wrap calls to the add_sensor method
                  on the parent FbfMasterController instance. In order to
                  disambiguate between sensors from describing different products
                  the associated proxy name is used as sensor prefix. For example
                  the "servers" sensor will be seen by clients connected to the
                  FbfMasterController server as "<proxy_name>-servers" (e.g.
                  "FBFUSE_1-servers").
        """
        prefix = "{}.".format(self._product_id)
        if sensor.name.startswith(prefix):
            self._parent.add_sensor(sensor)
        else:
            sensor.name = "{}{}".format(prefix,sensor.name)
            self._parent.add_sensor(sensor)
        self._managed_sensors.append(sensor)

    def setup_sensors(self):
        """
        @brief    Setup the default KATCP sensors.

        @note     As this call is made only upon an FBFUSE configure call a mass inform
                  is required to let connected clients know that the proxy interface has
                  changed.
        """
        self._state_sensor = Sensor.discrete(
            "state",
            description = "Denotes the state of this FBF instance",
            params = self.STATES,
            default = self.IDLE,
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._state_sensor)

        self._ca_address_sensor = Sensor.string(
            "configuration-authority",
            description = "The address of the server that will be deferred to for configurations",
            default = "",
            initial_status = Sensor.UNKNOWN)
        self.add_sensor(self._ca_address_sensor)

        self._available_antennas_sensor = Sensor.string(
            "available-antennas",
            description = "The antennas that are currently available for beamforming",
            default = self._antennas,
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._available_antennas_sensor)

        self._cbc_nbeams_sensor = Sensor.integer(
            "coherent-beam-count",
            description = "The number of coherent beams that this FBF instance can currently produce",
            default = 400,
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._cbc_nbeams_sensor)

        self._cbc_tscrunch_sensor = Sensor.integer(
            "coherent-beam-tscrunch",
            description = "The number time samples that will be integrated when producing coherent beams",
            default = 16,
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._cbc_tscrunch_sensor)

        self._cbc_fscrunch_sensor = Sensor.integer(
            "coherent-beam-fscrunch",
            description = "The number frequency channels that will be integrated when producing coherent beams",
            default = 1,
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._cbc_fscrunch_sensor)

        self._cbc_antennas_sensor = Sensor.string(
            "coherent-beam-antennas",
            description = "The antennas that will be used when producing coherent beams",
            default = self._antennas,
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._cbc_antennas_sensor)

        self._ibc_nbeams_sensor = Sensor.integer(
            "incoherent-beam-count",
            description = "The number of incoherent beams that this FBF instance can currently produce",
            default = 1,
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._ibc_nbeams_sensor)

        self._ibc_tscrunch_sensor = Sensor.integer(
            "incoherent-beam-tscrunch",
            description = "The number time samples that will be integrated when producing incoherent beams",
            default = 16,
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._ibc_tscrunch_sensor)

        self._ibc_fscrunch_sensor = Sensor.integer(
            "incoherent-beam-fscrunch",
            description = "The number frequency channels that will be integrated when producing incoherent beams",
            default = 1,
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._ibc_fscrunch_sensor)

        self._ibc_antennas_sensor = Sensor.string(
            "incoherent-beam-antennas",
            description = "The antennas that will be used when producing incoherent beams",
            default = self._antennas,
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._ibc_antennas_sensor)

        self._servers_sensor = Sensor.string(
            "servers",
            description = "The server instances currently allocated to this product",
            default = ",".join(["{s.hostname}:{s.port}".format(s=server) for server in self._servers]),
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._servers_sensor)

        self._delay_engine_sensor = Sensor.string(
            "delay-engine",
            description = "The address of the delay engine serving this product",
            default = "",
            initial_status = Sensor.UNKNOWN)
        self.add_sensor(self._delay_engine_sensor)
        self._parent.mass_inform(Message.inform('interface-changed'))

    def teardown_sensors(self):
        """
        @brief    Remove all sensors created by this product from the parent server.

        @note     This method is required for cleanup to stop the FBF sensor pool
                  becoming swamped with unused sensors.
        """
        for sensor in self._managed_sensors:
            self._parent.remove_sensor(sensor)
        self._parent.mass_inform(Message.inform('interface-changed'))

    @property
    def servers(self):
        return self._servers

    @property
    def capturing(self):
        return self.state == self.CAPTURING

    @property
    def idle(self):
        return self.state == self.IDLE

    @property
    def starting(self):
        return self.state == self.STARTING

    @property
    def stopping(self):
        return self.state == self.STOPPING

    @property
    def ready(self):
        return self.state == self.READY

    @property
    def preparing(self):
        return self.state == self.PREPARING

    @property
    def state(self):
        return self._state_sensor.value()

    def _verify_antennas(self, antennas):
        """
        @brief      Verify that a set of antennas is available to this instance.

        @param      antennas   A CSV list of antenna names
        """
        antennas_set = set([ant.name for ant in self._katpoint_antennas])
        requested_antennas = set(antennas)
        return requested_antennas.issubset(antennas_set)

    def set_configuration_authority(self, hostname, port):
        if self._ca_client:
            self._ca_client.stop()
        self._ca_client = KATCPClientResource(dict(
            name = 'configuration-authority-client',
            address = (hostname, port),
            controlled = True))
        self._ca_client.start()
        self._ca_address_sensor.set_value("{}:{}".format(hostname, port))

    @coroutine
    def get_ca_sb_configuration(self, sb_id):
        yield self._ca_client.until_synced()
        try:
            response = yield self._ca_client.req.get_schedule_block_configuration(self._proxy_name, sb_id)
        except Exception as error:
            log.error("Request for SB configuration to CA failed with error: {}".format(str(error)))
            raise error
        config_dict = json.loads(response.reply.arguments[1])
        sensor_map = {
            ('coherent-beams','nbeams') : self._cbc_nbeams_sensor,
            ('coherent-beams','antennas') : self._cbc_antennas_sensor,
            ('coherent-beams','tscrunch') : self._cbc_tscrunch_sensor,
            ('coherent-beams','fscrunch') : self._cbc_fscrunch_sensor,
            ('incoherent-beam','antennas') : self._ibc_antennas_sensor,
            ('incoherent-beam','tscrunch') : self._ibc_tscrunch_sensor,
            ('incoherent-beam','fscrunch') : self._ibc_fscrunch_sensor,
            }
        for key, subconfig in config_dict.items():
            for subkey, value in subconfig.items():
                sensor = sensor_map[(key, subkey)]
                log.info("CA set sensor {} to {}".format(sensor.name, value))
                sensor.set_value(value)

    def _sanitize_sb_configuration(self,  tscrunch, fscrunch, desired_nbeams, beam_granularity=None):

        # What are the key constraints:
        # 1. The data rate per multicast group
        # 2. The aggregate data rate out of the instrument (not as important)
        # 3. The processing limitations (has to be determined empirically)
        # 4. The number of multicast groups available
        # 5. The possible numbers of beams per multicast group (such that TUSE can receive N per node)
        # 6. Need to use at least 16 multicast groups
        # 7. Should have even flow across multicast groups, so same number of beams in each
        # 8. Multicast groups should be contiguous


        # Constants for data rates and bandwidths
        # these are hardcoded here for the moment but ultimately
        # they should be moved to a higher level or even dynamically
        # specified
        MAX_RATE_PER_MCAST = 6.8e9 # bits/s
        MAX_RATE_PER_SERVER = 4.375e9 # bits/s, equivalent to 280 Gb/s over 64 (virtual) nodes
        BANDWIDTH = 856e6 # MHz

        # Calculate the data rate for each beam assuming 8-bit packing and
        # no metadata overheads
        data_rate_per_beam = BANDWIDTH / tscrunch / fscrunch * 8 # bits/s
        log.debug("Data rate per coherent beam: {} Gb/s".format(data_rate_per_beam/1e9))

        # Calculate the maximum number of beams that will fit in one multicast
        # group assuming. Each multicast group must be receivable on a 10 GbE
        # connection so the max rate must be < 8 Gb/s
        max_beams_per_mcast = MAX_RATE_PER_MCAST // data_rate_per_beam
        log.debug("Maximum number of beams per multicast group: {}".format(int(max_beams_per_mcast)))

        if max_beams_per_mcast == 0:
            raise Exception("Data rate per beam is greater than the data rate per multicast group")

        # For instuments such as TUSE, they require a fixed number of beams per node. For their
        # case we assume that they will only acquire one multicast group per node and as such
        # the minimum number of beams per multicast group should be whatever TUSE requires.
        # Multicast groups can contain more beams than this but only in integer multiples of
        # the minimum
        if beam_granularity:
            if max_beams_per_mcast < beam_granularity:
                log.warning("Cannot fit {} beams into one multicast group, updating number of beams per multicast group to {}".format(
                    beam_granularity, max_beams_per_mcast))
                while np.modf(beam_granularity/max_beams_per_mcast)[0] != 0.0:
                    max_beams_per_mcast -= 1
                beam_granularity = max_beams_per_mcast
            beams_per_mcast = beam_granularity * (max_beams_per_mcast // beam_granularity)
            log.debug("Number of beams per multicast group, accounting for granularity: {}".format(int(beams_per_mcast)))
        else:
            beams_per_mcast = max_beams_per_mcast

        # Calculate the total number of beams that could be produced assuming the only
        # rate limit was that limit per multicast groups
        max_beams = self.n_mcast_groups * beams_per_mcast
        log.debug("Maximum possible beams (assuming on multicast group rate limit): {}".format(max_beams))

        if desired_nbeams > max_beams:
            log.warning("Requested number of beams is greater than theoretical maximum, "
                "updating setting the number of beams of beams to {}".format(max_beams))
            desired_nbeams = max_beams

        # Calculate the total number of multicast groups that are required to satisfy
        # the requested number of beams
        num_mcast_groups_required = round(desired_nbeams / beams_per_mcast + 0.5)
        log.debug("Number of multicast groups required for {} beams: {}".format(desired_nbeams, num_mcast_groups_required))
        actual_nbeams = num_mcast_groups_required * beams_per_mcast
        nmcast_groups = num_mcast_groups_required

        # Now we need to check the server rate limits
        if (actual_nbeams * data_rate_per_beam)/self.n_servers > MAX_RATE_PER_SERVER:
            log.warning("Number of beams limited by output data rate per server")
        actual_nbeams = MAX_RATE_PER_SERVER*self.n_servers // data_rate_per_beam
        log.info("Number of beams that can be generated: {}".format(actual_nbeams))
        return actual_nbeams

    @coroutine
    def get_ca_target_configuration(self, target):
        def ca_target_update_callback(received_timestamp, timestamp, status, value):
            # TODO, should we really reset all the beams or should we have
            # a mechanism to only update changed beams
            config_dict = json.loads(value)
            self.reset_beams()
            for target_string in config_dict.get('beams',[]):
                target = Target(target_string)
                self.add_beam(target)
            for tiling in config_dict.get('tilings',[]):
                target  = Target(tiling['target']) #required
                freq    = float(tiling.get('reference_frequency', 1.4e9))
                nbeams  = int(tiling['nbeams'])
                overlap = float(tiling.get('overlap', 0.5))
                epoch   = float(tiling.get('epoch', time.time()))
                self.add_tiling(target, nbeams, freq, overlap, epoch)
        yield self._ca_client.until_synced()
        try:
            response = yield self._ca_client.req.target_configuration_start(self._proxy_name, target.format_katcp())
        except Exception as error:
            log.error("Request for target configuration to CA failed with error: {}".format(str(error)))
            raise error
        if not response.reply.reply_ok():
            error = Exception(response.reply.arguments[1])
            log.error("Request for target configuration to CA failed with error: {}".format(str(error)))
            raise error
        yield self._ca_client.until_synced()
        sensor = self._ca_client.sensor["{}_beam_position_configuration".format(self._proxy_name)]
        sensor.register_listener(ca_target_update_callback)
        self._ca_client.set_sampling_strategy(sensor.name, "event")

    def configure_coherent_beams(self, nbeams, antennas, fscrunch, tscrunch):
        """
        @brief      Set the configuration for coherent beams producted by this instance

        @param      nbeams          The number of beams that will be produced for the provided product_id

        @param      antennas        A comma separated list of physical antenna names. Only these antennas will be used
                                    when generating coherent beams (e.g. m007,m008,m009). The antennas provided here must
                                    be a subset of the antennas in the current subarray. If not an exception will be
                                    raised.

        @param      fscrunch        The number of frequency channels to integrate over when producing coherent beams.

        @param      tscrunch        The number of time samples to integrate over when producing coherent beams.
        """
        if not self.idle:
            raise FbfStateError([self.IDLE], self.state)
        if not self._verify_antennas(parse_csv_antennas(antennas)):
            raise AntennaValidationError("Requested antennas are not a subset of the current subarray")
        self._cbc_nbeams_sensor.set_value(nbeams)
        #need a check here to determine if this is a subset of the subarray antennas
        self._cbc_fscrunch_sensor.set_value(fscrunch)
        self._cbc_tscrunch_sensor.set_value(tscrunch)
        self._cbc_antennas_sensor.set_value(antennas)

    def configure_incoherent_beam(self, antennas, fscrunch, tscrunch):
        """
        @brief      Set the configuration for incoherent beams producted by this instance

        @param      antennas        A comma separated list of physical antenna names. Only these antennas will be used
                                    when generating incoherent beams (e.g. m007,m008,m009). The antennas provided here must
                                    be a subset of the antennas in the current subarray. If not an exception will be
                                    raised.

        @param      fscrunch        The number of frequency channels to integrate over when producing incoherent beams.

        @param      tscrunch        The number of time samples to integrate over when producing incoherent beams.
        """
        if not self.idle:
            raise FbfStateError([self.IDLE], self.state)
        if not self._verify_antennas(parse_csv_antennas(antennas)):
            raise AntennaValidationError("Requested antennas are not a subset of the current subarray")
        #need a check here to determine if this is a subset of the subarray antennas
        self._ibc_fscrunch_sensor.set_value(fscrunch)
        self._ibc_tscrunch_sensor.set_value(tscrunch)
        self._ibc_antennas_sensor.set_value(antennas)

    def _beam_to_sensor_string(self, beam):
        return beam.target.format_katcp()

    @coroutine
    def target_start(self, target):
        if self._ca_client:
            yield self.get_ca_target_configuration(target)
        else:
            log.warning("No configuration authority is set, using default beam configuration")

    @coroutine
    def target_stop(self):
        if self._ca_client:
            sensor_name = "{}_beam_position_configuration".format(self._proxy_name)
            self._ca_client.set_sampling_strategy(sensor_name, "none")

    @coroutine
    def prepare(self):
        """
        @brief      Prepare the beamformer for streaming

        @detail     This method evaluates the current configuration creates a new DelayEngine
                    and passes a prepare call to all allocated servers.
        """
        if not self.idle:
            raise FbfStateError([self.IDLE], self.state)
        self._state_sensor.set_value(self.PREPARING)

        # Here we need to parse the streams and assign beams to streams:
        #mcast_addrs, mcast_port = parse_stream(self._streams['cbf.antenna_channelised_voltage']['i0.antenna-channelised-voltage'])

        if not self._ca_client:
            log.warning("No configuration authority found, using default configuration parameters")
        else:
            #TODO: get the schedule block ID into this call from somewhere (configure?)
            yield self.get_ca_sb_configuration("default_subarray")


        cbc_antennas_names = parse_csv_antennas(self._cbc_antennas_sensor.value())
        cbc_antennas = [self._antenna_map[name] for name in cbc_antennas_names]
        self._beam_manager = BeamManager(self._cbc_nbeams_sensor.value(), cbc_antennas)
        self._delay_engine = DelayEngine("127.0.0.1", 0, self._beam_manager)
        self._delay_engine.start()

        for server in self._servers:
            # each server will take 4 consequtive multicast groups
            pass

        # set up delay engine
        # compile kernels
        # start streaming
        self._delay_engine_sensor.set_value(self._delay_engine.bind_address)


        # Need to tear down the beam sensors here
        self._beam_sensors = []
        for beam in self._beam_manager.get_beams():
            sensor = Sensor.string(
                "coherent-beam-{}".format(beam.idx),
                description="R.A. (deg), declination (deg) and source name for coherent beam with ID {}".format(beam.idx),
                default=self._beam_to_sensor_string(beam),
                initial_status=Sensor.UNKNOWN)
            beam.register_observer(lambda beam, sensor=sensor:
                sensor.set_value(self._beam_to_sensor_string(beam)))
            self._beam_sensors.append(sensor)
            self.add_sensor(sensor)
        self._state_sensor.set_value(self.READY)

        # Only make this call if the the number of beams has changed
        self._parent.mass_inform(Message.inform('interface-changed'))

    def start_capture(self):
        if not self.ready:
            raise FbfStateError([self.READY], self.state)
        self._state_sensor.set_value(self.STARTING)
        """
        futures = []
        for server in self._servers:
            futures.append(server.req.start_capture())
        for future in futures:
            try:
                response = yield future
            except:
                pass
        """
        self._state_sensor.set_value(self.CAPTURING)

    def stop_beams(self):
        """
        @brief      Stops the beamformer servers streaming.
        """
        if not self.capturing:
            return
        self._state_sensor.set_value(self.STOPPING)
        for server in self._servers:
            #yield server.req.deconfigure()
            pass
        self._state_sensor.set_value(self.IDLE)

    def add_beam(self, target):
        """
        @brief      Specify the parameters of one managed beam

        @param      target      A KATPOINT target object

        @return     Returns the allocated Beam object
        """
        valid_states = [self.READY, self.CAPTURING, self.STARTING]
        if not self.state in valid_states:
            raise FbfStateError(valid_states, self.state)
        return self._beam_manager.add_beam(target)

    def add_tiling(self, target, number_of_beams, reference_frequency, overlap, epoch):
        """
        @brief   Add a tiling to be managed

        @param      target      A KATPOINT target object

        @param      reference_frequency     The reference frequency at which to calculate the synthesised beam shape,
                                            and thus the tiling pattern. Typically this would be chosen to be the
                                            centre frequency of the current observation.

        @param      overlap         The desired overlap point between beams in the pattern. The overlap defines
                                    at what power point neighbouring beams in the tiling pattern will meet. For
                                    example an overlap point of 0.1 corresponds to beams overlapping only at their
                                    10%-power points. Similarly a overlap of 0.5 corresponds to beams overlapping
                                    at their half-power points. [Note: This is currently a tricky parameter to use
                                    when values are close to zero. In future this may be define in sigma units or
                                    in multiples of the FWHM of the beam.]

        @returns    The created Tiling object
        """
        valid_states = [self.READY, self.CAPTURING, self.STARTING]
        if not self.state in valid_states:
            raise FbfStateError(valid_states, self.state)
        tiling = self._beam_manager.add_tiling(target, number_of_beams, reference_frequency, overlap)
        tiling.generate(self._katpoint_antennas, epoch)
        return tiling

    def reset_beams(self):
        """
        @brief  reset and deallocate all beams and tilings managed by this instance

        @note   All tiling will be lost on this call and must be remade for subsequent observations
        """
        valid_states = [self.READY, self.CAPTURING, self.STARTING]
        if not self.state in valid_states:
            raise FbfStateError(valid_states, self.state)
        self._beam_manager.reset()


@coroutine
def on_shutdown(ioloop, server):
    log.info("Shutting down server")
    yield server.stop()
    ioloop.stop()

def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--host', dest='host', type=str,
        help='Host interface to bind to')
    parser.add_option('-p', '--port', dest='port', type=long,
        help='Port number to bind to')
    parser.add_option('', '--log_level',dest='log_level',type=str,
        help='Port number of status server instance',default="INFO")
    parser.add_option('', '--dummy',action="store_true", dest='dummy',
        help='Set status server to dummy')
    (opts, args) = parser.parse_args()
    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logger = logging.getLogger('mpikat')
    logging.basicConfig(format=FORMAT)
    logger.setLevel(opts.log_level.upper())
    logging.getLogger('katcp').setLevel('INFO')
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting FbfMasterController instance")
    server = FbfMasterController(opts.host, opts.port, dummy=opts.dummy)
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, server))
    def start_and_display():
        server.start()
        log.info("Listening at {0}, Ctrl-C to terminate server".format(server.bind_address))

    ioloop.add_callback(start_and_display)
    ioloop.start()

if __name__ == "__main__":
    main()

