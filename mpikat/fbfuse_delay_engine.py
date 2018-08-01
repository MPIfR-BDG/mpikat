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
import json
import time
import mosaic
from katcp import Sensor, AsyncDeviceServer
from katcp.kattypes import request, return_reply, Float
from katpoint import Antenna

log = logging.getLogger("mpikat.delay_engine")

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