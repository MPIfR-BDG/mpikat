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
import time
import cPickle
from threading import Lock
from optparse import OptionParser
from tornado.gen import coroutine, Return
from katcp import Sensor, AsyncReply
from katcp.kattypes import request, return_reply, Int, Str, Float
from katpoint import Antenna, Target
from mpikat.core.master_controller import (
    MasterController, ProductLookupError, ProductExistsError)
from mpikat.core.ip_manager import IpRangeManager, ip_range_from_stream
from mpikat.core.utils import parse_csv_antennas
from mpikat.meerkat.katportalclient_wrapper import (
    KatportalClientWrapper, SubarrayActivity)
from mpikat.meerkat.fbfuse import FbfWorkerPool, FbfProductController
from mpikat.meerkat.test.antennas import ANTENNAS as DEFAULT_ANTENNA_MODELS
from mpikat.meerkat.fbfuse.fbfuse_feng_subscription_manager import (
    FengToFbfMapper,)

# ?halt message means shutdown everything and power off all machines

log = logging.getLogger("mpikat.fbfuse_master_controller")
lock = Lock()

FBF_IP_RANGE = "spead://239.11.1.0+127:7147"
CONFIG_PICKLE_FILE = "/tmp/fbfuse_config.pickle"
VALID_NCHANS = [1024, 4096, 32768]


class FbfMasterController(MasterController):
    """This is the main KATCP interface for the FBFUSE
    multi-beam beamformer on MeerKAT.

    This interface satisfies the following ICDs:
    CAM-FBFUSE: <link>
    TUSE-FBFUSE: <link>
    """
    VERSION_INFO = ("mpikat-fbf-api", 0, 1)
    BUILD_INFO = ("mpikat-fbf-implementation", 0, 1, "rc1")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]

    def __init__(self, ip, port, dummy=True, ip_range=FBF_IP_RANGE):
        """
        @brief       Construct new FbfMasterController instance

        @params  ip       The IP address on which the server should listen
        @params  port     The port that the server should bind to
        @params  dummy    Specifies if the instance is running in a dummy mode

        @note   In dummy mode, the controller will act as a mock interface only, sending no requests to nodes.
                A valid node pool must still be provided to the instance, but this may point to non-existent nodes.

        """
        self._ip_pool = IpRangeManager(ip_range_from_stream(ip_range))
        self._katportal_wrapper_type = KatportalClientWrapper
        super(FbfMasterController, self).__init__(ip, port, FbfWorkerPool())
        self._dummy = dummy
        if self._dummy:
            for ii in range(0):
                self._server_pool.add("127.0.0.1", 50000+ii)
        self._last_configure_arguments = None
        self._feng_subscription_manager = FengToFbfMapper()

    def setup_sensors(self):
        """
        @brief  Set up monitoring sensors.
        """
        super(FbfMasterController, self).setup_sensors()
        self._ip_pool_sensor = Sensor.string(
            "output-ip-range",
            description="The multicast address allocation for coherent beams",
            default=self._ip_pool.format_katcp(),
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._ip_pool_sensor)

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
        log.info("Received configure request")
        @coroutine
        def configure_coroutine_wrapper():
            try:
                yield self.configure(product_id, antennas_csv, n_channels,
                                     streams_json, proxy_name)
            except Exception as error:
                log.error("Configure request failed: {}".format(str(error)))
                req.reply("fail", str(error))
            else:
                log.info("Configure request successful")
                req.reply("ok")
        self.ioloop.add_callback(configure_coroutine_wrapper)
        raise AsyncReply

    @request()
    @return_reply()
    def request_reconfigure(self, req):
        """
        @brief   Reconfigure the current instance either from in memory
                 configuration information or from disk.
        """
        log.info("Received reconfigure request")
        @coroutine
        def reconfigure_coroutine_wrapper():
            try:
                yield self.reconfigure()
            except Exception as error:
                log.error("Reconfigure request failed: {}".format(str(error)))
                req.reply("fail", "Unable to reconfigure with error: {}".format(
                    str(error)))
            else:
                log.info("Reconfigure request successful")
                req.reply("ok",)
        self.ioloop.add_callback(reconfigure_coroutine_wrapper)
        raise AsyncReply

    @coroutine
    def reconfigure(self):
        """
        @brief   Internal implementation of reconfigure
        """
        if self._last_configure_arguments:
            log.info("Retreiving config information from memory")
            args = self._last_configure_arguments
        else:
            log.info("Retreiving config information from disk")
            with open(CONFIG_PICKLE_FILE, "r") as f:
                args = cPickle.load(f)
        product_id = args[0]
        try:
            yield self.deconfigure(product_id)
        except Exception as e:
            log.warning(("Unable to deconfigure prior product with ID"
                         " '{}': {}").format(product_id, str(e)))
        yield self.configure(*args)

    @coroutine
    def configure(self, product_id, antennas_csv, n_channels, streams_json, proxy_name):
        """
        @brief     Internal implementation of configure.

        @detail    This function is used to break the configure logic from only being
                   accessible through a KATCP request. For details for the parameters
                   see the "request_configure" method implemented above
        """
        with open(CONFIG_PICKLE_FILE, "w") as f:
            cPickle.dump((product_id, antennas_csv, n_channels,
                          streams_json, proxy_name), f)
        self._last_configure_arguments = (product_id, antennas_csv, n_channels,
                                          streams_json, proxy_name)
        msg = ("Configuring new FBFUSE product",
               "Product ID: {}".format(product_id),
               "Antennas: {}".format(antennas_csv),
               "Nchannels: {}".format(n_channels),
               "Streams: {}".format(streams_json),
               "Proxy name: {}".format(proxy_name))
        log.info("\n".join(msg))
        # Test if product_id already exists
        if product_id in self._products:
            raise ProductExistsError("FBF already has a configured product with ID: {}".format(product_id))
        # Determine number of nodes required based on number of antennas in subarray
        # Note this is a poor way of handling this that may be updated later. In theory
        # there is a throughput measure as a function of bandwidth, polarisations and number
        # of antennas that allows one to determine the number of nodes to run. Currently we
        # just assume one antennas worth of data per NIC on our servers, so two antennas per
        # node.
        antennas = parse_csv_antennas(antennas_csv)
        if n_channels not in VALID_NCHANS:
            raise Exception(("The provided number of channels ({}) is not "
                             "valid. Valid options are {}").format(
                             n_channels, VALID_NCHANS))
        streams = json.loads(streams_json)
        try:
            streams['cam.http']['camdata']
            # Need to check for endswith('.antenna-channelised-voltage') as the i0 is not
            # guaranteed to stay the same.
            # i0 = instrument name
            # Need to keep this for future sensor lookups
            streams['cbf.antenna_channelised_voltage']
        except KeyError as error:
            raise KeyError(("JSON streams object does not contain "
                            "required key: {}").format(str(error)))

        for key, value in streams['cbf.antenna_channelised_voltage'].items():
            if key.endswith('.antenna-channelised-voltage'):
                instrument_name, _ = key.split('.')
                feng_stream_name = key
                feng_groups = value
                log.debug("Parsed instrument name from streams: {}".format(
                    instrument_name))
                break
        else:
            raise Exception("Could not determine instrument name "
                            "(e.g. 'i0') from streams")
        kpc = self._katportal_wrapper_type(streams['cam.http']['camdata'])
        # Get all antenna observer strings
        log.debug("Fetching katpoint string for antennas: {}".format(
            antennas))
        try:
            response = yield kpc.get_observer_strings(antennas)
        except Exception as error:
            log.exception("Error on katportalclient call: {}".format(
                    str(error)))
            response = {}
        observers = []
        for antenna in antennas:
            if antenna in response:
                observer = response[antenna]
            else:
                log.warning("Falling back on default pointing model for "
                            "antenna '{}'".format(antenna))
                observer = DEFAULT_ANTENNA_MODELS[antenna]
            log.debug("Fetched katpoint antenna: {}".format(observer))
            observers.append(Antenna(observer))
        # Get bandwidth, cfreq, sideband, f-eng mapping
        # TODO: Also get sync-epoch
        log.debug("Fetching F-engine and subarray configuration information")
        bandwidth_future = kpc.get_bandwidth(feng_stream_name)
        cfreq_future = kpc.get_cfreq(feng_stream_name)
        sideband_future = kpc.get_sideband(feng_stream_name)
        feng_antenna_map_future = kpc.get_antenna_feng_id_map(
            instrument_name, antennas)
        sync_epoch_future = kpc.get_sync_epoch()
        bandwidth = yield bandwidth_future
        cfreq = yield cfreq_future
        sideband = yield sideband_future
        feng_antenna_map = yield feng_antenna_map_future
        sync_epoch = yield sync_epoch_future
        feng_config = {
            'bandwidth': bandwidth,
            'centre-frequency': cfreq,
            'sideband': sideband,
            'feng-antenna-map': feng_antenna_map,
            'sync-epoch': sync_epoch,
            'nchans': n_channels
        }
        for key, value in feng_config.items():
            log.debug("{}: {}".format(key, value))

        log.info("Starting subarray activity tracker")
        activity_tracker = SubarrayActivity(streams['cam.http']['camdata'])
        product = FbfProductController(
            self, product_id, observers, n_channels,
            feng_groups, proxy_name, feng_config, activity_tracker)
        self._products[product_id] = product
        self._update_products_sensor()
        log.debug("Configured FBFUSE instance with ID: {}".format(product_id))

    @coroutine
    def deprovision_beams(self, product_id):
        log.info("Deprovisioning beams on FBFUSE instace with ID '{}'".format(
            product_id))
        # Test if product exists
        product = self._get_product(product_id)
        yield product.reset_sb_configuration()

    @request(Str())
    @return_reply()
    def request_deprovision_beams(self, req, product_id):
        """
        @brief      Deprovision beams on an FBFUSE product.

        @note       This is similar to a deconfigure, but it will not delete
                    the product, only deallocate its resources.

        @param      req               A katcp request object

        @param      product_id        This is a name for the data product, used to track which subarray is being deconfigured.
                                      For example "array_1_bc856M4k".

        @return     katcp reply object [[[ !deprovision-beams ok | (fail [error description]) ]]]
        """
        log.info("Received deprovision-beams request")
        @coroutine
        def deprovision_beams_wrapper():
            try:
                yield self.deprovision_beams(product_id)
            except Exception as error:
                log.error("deprovision-beams request failed: {}".format(
                    str(error)))
                req.reply("fail", str(error))
            else:
                log.info("deprovision-beams request successful")
                req.reply("ok",)
        self.ioloop.add_callback(deprovision_beams_wrapper)
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
        log.info("Received deconfigure request")
        @coroutine
        def deconfigure_wrapper():
            try:
                yield self.deconfigure(product_id)
            except Exception as error:
                log.error("Deconfigure request failed: {}".format(
                    str(error)))
                req.reply("fail", str(error))
            else:
                log.info("Deconfigure request successful")
                req.reply("ok",)
        self.ioloop.add_callback(deconfigure_wrapper)
        raise AsyncReply

    @coroutine
    def deconfigure(self, product_id):
        """
        @brief     Internal implementation of deconfigure.

        @detail    This function is used to break the deconfigure logic from only being
                   accessible through a KATCP request.
        """
        log.info("Deconfiguring FBFUSE instace with ID '{}'".format(product_id))
        # Test if product exists
        product = self._get_product(product_id)
        try:
            yield product.deconfigure()
        except Exception as error:
            log.exception(("Encountered error while deconfiguring product "
                           "'{}': {}").format(product_id, str(error)))
            log.warning("Forcing product deconfigure. This action may have unintented consequences"
                        " if any worker servers are still allocated to the product")
        del self._products[product_id]
        self._update_products_sensor()

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
        log.info("Received target-start request for target: {}".format(target))
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            log.error("target-start request failed with error: {}".format(
                str(error)))
            raise Return("fail", str(error))
        try:
            target = Target(target)
        except Exception as error:
            raise Return(("fail", str(error)))
        yield product.target_start(target)
        raise Return(("ok",))

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
        log.info("Received capture-start request")
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            log.error("Capture-start request failed: {}".format(str(error)))
            return ("fail", str(error))

        @coroutine
        def start():
            try:
                yield product.capture_start()
            except Exception as error:
                log.exception("Capture-start request failed: {}".format(str(error)))
                req.reply("fail", str(error))
            else:
                log.info("Capture-start request successful")
                req.reply("ok",)
        self.ioloop.add_callback(start)
        raise AsyncReply

    @request(Str(), Str())
    @return_reply()
    def request_provision_beams(self, req, product_id, sb_id):
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

        @param      sb_id             Schedule block ID for the commencing schedule block

        @return     katcp reply object [[[ !start-beams ok | (fail [error description]) ]]]
        """
        # Note: the state of the product won't be updated until the start call hits the top of the
        # event loop. It may be preferable to keep a self.starting_future object and yield on it
        # in capture-start if it exists. The current implementation may or may not be a bug...
        log.info("Received provision-beams request")
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            log.error("Provision-beams request failed: {}".format(
                str(error)))
            return ("fail", str(error))
        self.ioloop.add_callback(lambda: product.prepare(sb_id))
        log.info("Provision-beams request successful")
        return ("ok",)

    @request(Str())
    @return_reply()
    def request_capture_stop(self, req, product_id):
        """
        @brief      Stop FBFUSE streaming

        @param      product_id      This is a name for the data product, used to track which subarray is being deconfigured.
                                    For example "array_1_bc856M4k".
        """
        log.info("Received capture-stop request")
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            log.error("Capture-stop request failed: {}".format(
                str(error)))
            return ("fail", str(error))

        @coroutine
        def stop():
            try:
                yield product.capture_stop()
            except Exception as error:
                log.exception("Capture-stop request failed: {}".format(
                    str(error)))
                req.reply("fail", str(error))
            else:
                log.info("Capture-stop request successful")
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
        log.info("Received set-configuration-authority request")
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            log.error("Set-configuration-authority request failed: {}".format(
                str(error)))
            return ("fail", str(error))
        try:
            product.set_configuration_authority(hostname, port)
        except Exception as error:
            log.exception("Set-configuration-authority request failed: {}".format(
                str(error)))
            return ("fail", str(error))
        log.info("Set-configuration-authority request successful")
        return ("ok",)

    @request(Str(), Float(), Float())
    @return_reply()
    def request_set_levels(self, req, product_id, input_level, output_level):
        """
        @brief    Set the input and output levels for FBFUSE

        @param      req             A katcp request object

        @param      product_id      This is a name for the data product, used
                                    to track which subarray is being
                                    deconfigured. For example "array_1_bc856M4k".

        @param    input_level  The standard deviation of the data
                               from the F-engines.

        @param    output_level  The standard deviation of the data
                                output from FBFUSE.
        """
        log.info("Received set-levels request")
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            log.error("set-levels request failed: {}".format(
                str(error)))
            return ("fail", str(error))

        @coroutine
        def set_levels_wrapper():
            try:
                yield product.set_levels(input_level, output_level)
            except Exception as error:
                log.exception("set-levels request failed: {}".format(
                    str(error)))
                req.reply("fail", str(error))
            else:
                log.info("set-levels request successful")
                req.reply("ok",)

        self.ioloop.add_callback(set_levels_wrapper)
        raise AsyncReply

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
            product.reset_beams()
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
    def request_set_default_sb_configuration(self, req, product_id, sb_id, target):
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

    @request()
    @return_reply()
    def request_register_default_worker_servers(self, req):
        """
        @brief      Add default FBFUSE nodes to the server pool
        """
        for idx in range(32):
            self._server_pool.add("fbfpn{:02d}.mpifr-be.mkat.karoo.kat.ac.za".format(idx), 6000)
            self._server_pool.add("fbfpn{:02d}.mpifr-be.mkat.karoo.kat.ac.za".format(idx), 6001)
        return ("ok",)

    @request(Str())
    @return_reply(Str())
    def request_beam_positions(self, req, product_id):
        """
        @brief      Add default FBFUSE nodes to the server pool
        """
        try:
            product = self._get_product(product_id)
        except ProductLookupError as error:
            return ("fail", str(error))
        beam_dict = {}
        beams = product._beam_manager.get_beams()
        for beam in beams:
            beam_dict[beam.idx] = beam.target.format_katcp()
        return ("ok", json.dumps(beam_dict))


@coroutine
def on_shutdown(ioloop, server):
    log.info("Shutting down server")
    yield server.stop()
    ioloop.stop()


def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option(
        '-H', '--host', dest='host', type=str,
        help='Host interface to bind to')
    parser.add_option(
        '-p', '--port', dest='port', type=int,
        help='Port number to bind to')
    parser.add_option(
        '', '--log_level', dest='log_level', type=str,
        help='Port number of status server instance', default="INFO")
    parser.add_option(
        '', '--dummy', action="store_true", dest='dummy',
        help='Set status server to dummy')
    (opts, args) = parser.parse_args()
    logger = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt=("[ %(levelname)s - %(asctime)s - %(name)s - "
             "%(filename)s:%(lineno)s] %(message)s"),
        level=opts.log_level.upper(),
        logger=logger)
    logging.getLogger('katcp').setLevel('INFO')
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting FbfMasterController instance")
    server = FbfMasterController(opts.host, opts.port, dummy=opts.dummy)
    signal.signal(
        signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
            on_shutdown, ioloop, server))

    def start_and_display():
        server.start()
        log.info("Listening at {0}, Ctrl-C to terminate server".format(
            server.bind_address))

    ioloop.add_callback(start_and_display)
    ioloop.start()


if __name__ == "__main__":
    main()
