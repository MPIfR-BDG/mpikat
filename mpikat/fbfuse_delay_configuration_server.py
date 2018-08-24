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
from katcp import Sensor, AsyncDeviceServer
from katpoint import Antenna

log = logging.getLogger("mpikat.delay_configuration_server")

class DelayConfigurationServer(AsyncDeviceServer):
    """A server for maintining delay models used
    by FbfWorkerServers.
    """
    VERSION_INFO = ("delay-configuration-server-api", 0, 1)
    BUILD_INFO = ("delay-configuration-server-implementation", 0, 1, "rc1")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]

    def __init__(self, ip, port, beam_manager):
        """
        @brief  Create a new DelayConfigurationServer instance

        @param   ip   The interface that the DelayConfigurationServer should serve on

        @param   port The port that the DelayConfigurationServer should serve on

        @param   beam_manager  A BeamManager instance that will be used to create delays
        """
        self._beam_manager = beam_manager
        self._reference_target = None
        super(DelayConfigurationServer, self).__init__(ip,port)

    def setup_sensors(self):
        """
        @brief    Set up monitoring sensors.
        """
        self._target_sensors = []
        for beam in self._beam_manager.get_beams():
            sensor = Sensor.string(
                "{}-target".format(beam.idx),
                description="Target for beam {}".format(beam.idx),
                default=beam.target.format_katcp(),
                initial_status=Sensor.UNKNOWN)
            self.add_sensor(sensor)
            beam.register_observer(lambda beam, sensor=sensor:
                sensor.set_value(beam.target.format_katcp()))

        antenna_map = {a.name:a.format_katcp() for a in self._beam_manager.antennas}
        self._antennas_sensor = Sensor.string(
            "antennas",
            description="JSON breakdown of the antennas (in KATPOINT format) associated with this delay engine",
            default=json.dumps(antenna_map),
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._antennas_sensor)

        self._phase_reference_sensor = Sensor.string(
            "phase-reference",
            description="A KATPOINT target string denoting the F-engine phasing centre",
            default="unset,radec,0,0",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._phase_reference_sensor)

        reference_antenna = Antenna("reference,{ref.lat},{ref.lon},{ref.elev}".format(
            ref=self._beam_manager.antennas[0].ref_observer))
        self._reference_antenna_sensor = Sensor.string(
            "reference-antenna",
            description="A KATPOINT antenna string denoting the reference antenna",
            default=reference_antenna.format_katcp(),
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._reference_antenna_sensor)

    def start(self):
        super(DelayConfigurationServer, self).start()
