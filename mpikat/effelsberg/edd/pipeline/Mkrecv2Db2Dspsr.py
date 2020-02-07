"""
Copyright (c) 2019 Jason Wu <jwu@mpifr-bonn.mpg.de>

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
from pipeline_register import register_pipeline
from ExecuteCommand import ExecuteCommand

import logging
import tempfile
import json
from tornado import gen
import os
import time
from astropy.time import Time
from mpikat.effelsberg.edd.pipeline.dada import render_dada_header, make_dada_key_string
import shlex
import threading
import base64
from katcp import Sensor
log = logging.getLogger("mpikat.effelsberg.edd.pipeline.Mkrecv2Db2Dsps")
log.setLevel('DEBUG')

RUN = True


#        "args": "-n 64 -b 67108864 -p -l",
CONFIG = {
        "base_output_dir": os.getcwd(),
        "dspsr_params":
        {
            "args": "-L 10 -r -F 256:D -x 16384 -minram 1024"
        },
        "dada_db_params":
        {
            "args": "-n 8 -b 671088640 -p -l",
            "key": "dada"
        },
        "dada_header_params":
        {
            "filesize": 32000000000,
            "telescope": "Effelsberg",
            "instrument": "asterix",
            "frequency_mhz": 1370,
            "receiver_name": "P200-3",
            "mc_source": "239.2.1.154",
            "bandwidth": -162.5,
            "tsamp": 0.04923076923076923,
            "nbit": 8,
            "ndim": 2,
            "npol": 2,
            "nchan": 8,
            "resolution": 1,
            "dsb": 1
        }
    }


"""
Central frequency of each band should be with BW of 162.5
239.2.1.150 2528.90625
239.2.1.151 2366.40625
239.2.1.152 2203.9075
239.2.1.153 2041.40625
239.2.1.154 1878.90625
239.2.1.155 1716.405
239.2.1.156 1553.9075
239.2.1.157 1391.40625
"""

sensors = {"ra": 123, "dec": -10, "source-name": "J1939+2134",
           "scannum": 0, "subscannum": 1}



class PulsarPipelineKeyError(Exception):
    pass


class PulsarPipelineError(Exception):
    pass



@register_pipeline("DspsrPipelineSrxdev")
class Mkrecv2Db2Dspsr(object):
    """@brief dspsr pipeline class."""

    def __del__(self):
        class_name = self.__class__.__name__

    def notify(self):
        """@brief callback function."""
        for callback in self.callbacks:
            callback(self._state, self)

    @property
    def state(self):
        """@brief property of the pipeline state."""
        return self._state

    @state.setter
    def state(self, value):
        self._state = value
        self.notify()

    def __init__(self):
        """@brief initialize the pipeline."""
        self.callbacks = set()
        self._state = "idle"
        self._sensors = []
        self._volumes = ["/tmp/:/scratch/"]
        self._dada_key = None
        self._config = None
        self._source_config = None
        self._dspsr = None
        self._mkrecv_ingest_proc = None
        self.setup_sensors()

    def setup_sensors(self):
        """
        @brief Setup monitoring sensors
        """
        self._tscrunch = Sensor.string(
            "tscrunch_PNG",
            description="tscrunch png",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.sensors.append(self._tscrunch)

        self._fscrunch = Sensor.string(
            "fscrunch_PNG",
            description="fscrunch png",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.sensors.append(self._fscrunch)

    @property
    def sensors(self):
        return self._sensors

    def _decode_capture_stdout(self, stdout, callback):
        log.debug('{}'.format(str(stdout)))

    def _save_capture_stdout(self, stdout, callback):
        with open("{}.par".format(self._source_config["source-name"]), "a") as file:
            file.write('{}\n'.format(str(stdout)))

    def _handle_execution_returncode(self, returncode, callback):
        log.debug(returncode)

    def _handle_execution_stderr(self, stderr, callback):
        log.info(stderr)

#    def _add_tscrunch_to_sensor(self, png_blob, callback):
#        self._tscrunch.set_value(png_blob)
#
#    def _add_fscrunch_to_sensor(self, png_blob, callback):
#        self._fscrunch.set_value(png_blob)

    #@gen.coroutine
    def configure(self, config_json):
        """@brief destroy any ring buffer and create new ring buffer."""
        if self.state == "idle":
            self.state = "configuring"
            self._pipeline_config = json.loads(config_json)
            self._config = CONFIG
            #self._dada_key = self._pipeline_config["key"]
            self._dada_key = "dada"
            try:
                self.deconfigure()
            except Exception as error:
                raise PulsarPipelineError(str(error))
            cmd = "dada_db -k {key} {args}".format(key=self._dada_key,
                                                   args=self._config["dada_db_params"]["args"])
            # cmd = "dada_db -k {key} {args}".format(**
            #                                       self._config["dada_db_params"])
            log.debug("Running command: {0}".format(cmd))
            self._create_ring_buffer = ExecuteCommand(
                cmd, outpath=None, resident=False)
            self._create_ring_buffer.stdout_callbacks.add(
                self._decode_capture_stdout)
            self._create_ring_buffer._process.wait()
            self.state = "ready"
        else:
            log.error(
                "pipleine state is not in state = ready, cannot start the pipeline")

    #@gen.coroutine
    def start(self, config_json):
        """@brief start the dspsr instance then turn on dada_junkdb instance."""
        if self.state == "ready":
            self.state = "starting"
            self._source_config = json.loads(config_json)
            self.frequency_mhz = self._pipeline_config["central_freq"]
            header = self._config["dada_header_params"]
            header["ra"], header["dec"], header["key"] = self._source_config[
                "ra"], self._source_config["dec"], self._dada_key
            header["mc_source"], header["frequency_mhz"] = self._pipeline_config[
                "mc_source"], self.frequency_mhz
            source_name = self._source_config["source-name"]
            cpu_numbers = "2,3"
            #cpu_numbers = self._pipeline_config["cpus"]
            #cuda_number = self._pipeline_config["cuda"]
            cuda_number = "0"
            try:
                header["sync_time"] = self._source_config["sync_time"]
                header["sample_clock"] = self._source_config["sample_clock"]
            except:
                pass
            log.debug("Unpacked config: {}".format(self._source_config))
            try:
                self.source_name = source_name.split("_")[0]
            except Exception as error:
                raise PulsarPipelineError(str(error))
            header["source_name"] = self.source_name
            header["obs_id"] = "{0}_{1}".format(
                sensors["scannum"], sensors["subscannum"])
            tstr = Time.now().isot.replace(":", "-")
            ####################################################
            #SETTING UP THE INPUT AND SCRUNCH DATA DIRECTORIES #
            ####################################################
            in_path = os.path.join("/data/jason/", self.source_name,
                                   str(self.frequency_mhz), tstr, "raw_data")
            out_path = os.path.join(
                "/data/jason/", self.source_name, str(self.frequency_mhz), tstr, "combined_data")
            self.out_path = out_path
            log.debug("Creating directories")
            cmd = "mkdir -p {}".format(in_path)
            log.debug("Command to run: {}".format(cmd))
            log.debug("Current working directory: {}".format(os.getcwd()))
            self._create_workdir_in_path = ExecuteCommand(
                cmd, outpath=None, resident=False)
            self._create_workdir_in_path.stdout_callbacks.add(
                self._decode_capture_stdout)
            self._create_workdir_in_path._process.wait()
            cmd = "mkdir -p {}".format(out_path)
            log.debug("Command to run: {}".format(cmd))
            log.info("Createing data directory {}".format(self.out_path))
            self._create_workdir_out_path = ExecuteCommand(
                cmd, outpath=None, resident=False)
            self._create_workdir_out_path.stdout_callbacks.add(
                self._decode_capture_stdout)
            self._create_workdir_out_path._process.wait()
            os.chdir(in_path)
            log.debug("Change to workdir: {}".format(os.getcwd()))
            log.debug("Current working directory: {}".format(os.getcwd()))
            ####################################################
            #CREATING THE PARFILE WITH PSRCAT                  #
            ####################################################
            cmd = "psrcat -E {source_name}".format(
                source_name=self.source_name)
            log.debug("Command to run: {}".format(cmd))
            self.psrcat = ExecuteCommand(cmd, outpath=None, resident=False)
            self.psrcat.stdout_callbacks.add(
                self._save_capture_stdout)
            self.psrcat.stderr_callbacks.add(
                self._handle_execution_stderr)
            time.sleep(3)
            ####################################################
            #CREATING THE PREDICTOR WITH TEMPO2                #
            ####################################################
            cmd = 'tempo2 -f {}.par -pred "Effelsberg {} {} {} {} 8 2 3599.999999999"'.format(
                self.source_name, Time.now().mjd - 2, Time.now().mjd + 2, float(self._pipeline_config["central_freq"]) - (162.5 / 2), float(self._pipeline_config["central_freq"]) + (162.5 / 2))
            log.debug("Command to run: {}".format(cmd))
            self.tempo2 = ExecuteCommand(cmd, outpath=None, resident=False)
            self.tempo2.stdout_callbacks.add(
                self._decode_capture_stdout)
            self.tempo2.stderr_callbacks.add(
                self._handle_execution_stderr)
            ####################################################
            #CREATING THE DADA HEADERFILE                      #
            ####################################################
            dada_header_file = tempfile.NamedTemporaryFile(
                mode="w",
                prefix="edd_dada_header_",
                suffix=".txt",
                dir=os.getcwd(),
                delete=False)
            log.debug(
                "Writing dada header file to {0}".format(
                    dada_header_file.name))
            header_string = render_dada_header(header)
            dada_header_file.write(header_string)
            dada_key_file = tempfile.NamedTemporaryFile(
                mode="w",
                prefix="dada_keyfile_",
                suffix=".key",
                dir=os.getcwd(),
                delete=False)
            log.debug("Writing dada key file to {0}".format(
                dada_key_file.name))
            key_string = make_dada_key_string(self._dada_key)
            dada_key_file.write(make_dada_key_string(self._dada_key))
            log.debug("Dada key file contains:\n{0}".format(key_string))
            dada_header_file.close()
            dada_key_file.close()
            time.sleep(3)
            ####################################################
            #STARTING DSPSR                                    #
            ####################################################
            cmd = "dspsr {args} -cpu {cpus} -cuda {cuda_number} -P {predictor} -E {parfile} {keyfile}".format(
                args=self._config["dspsr_params"]["args"],
                predictor="{}/t2pred.dat".format(in_path),
                parfile="{}/{}.par".format(in_path, self.source_name),
                cpus=cpu_numbers,
                cuda_number=cuda_number,
                keyfile=dada_key_file.name)
            log.debug("Running command: {0}".format(cmd))
            log.info("Staring DSPSR")
            self._dspsr = ExecuteCommand(cmd, outpath=None, resident=True)
            self._dspsr.stdout_callbacks.add(
                self._decode_capture_stdout)
            self._dspsr.stderr_callbacks.add(
                self._handle_execution_stderr)
            ####################################################
            #STARTING MKRECV                                   #
            ####################################################
            cmd = "mkrecv_nt --header {} --dada-mode 4".format(
                dada_header_file.name)
            log.debug("Running command: {0}".format(cmd))
            log.info("Staring MKRECV")
            self._mkrecv_ingest_proc = ExecuteCommand(
                cmd, outpath=None, resident=True)
            self._mkrecv_ingest_proc.stdout_callbacks.add(
                self._decode_capture_stdout)
            ####################################################
            #STARTING ARCHIVE MONITOR                          #
            ####################################################
            cmd = "python /home/psr/software/mpikat/mpikat/effelsberg/edd/pipeline/archive_directory_monitor.py -i {} -o {}".format(
                in_path, out_path)
            log.debug("Running command: {0}".format(cmd))
            log.info("Staring archive monitor")
            self._archive_directory_monitor = ExecuteCommand(
                cmd, outpath=out_path, resident=True)
            self._archive_directory_monitor.stdout_callbacks.add(
                self._decode_capture_stdout)
            self._archive_directory_monitor.fscrunch_callbacks.add(
                self._add_fscrunch_to_sensor)
            self._archive_directory_monitor.tscrunch_callbacks.add(
                self._add_tscrunch_to_sensor)
            self.state = "running"
        else:
            log.error(
                "pipleine state is not in state = ready, cannot start the pipeline")

    #@gen.coroutine
    def stop(self):
        """@brief stop the dada_junkdb and dspsr instances."""
        if self.state == 'running':
            log.debug("Stopping")
            self._timeout = 10
            process = [self._mkrecv_ingest_proc,
                       self._dspsr, self._archive_directory_monitor]
            for proc in process:
                proc.set_finish_event()
                proc.finish()
                log.debug(
                    "Waiting {} seconds for proc to terminate...".format(self._timeout))
                now = time.time()
                while time.time() - now < self._timeout:
                    retval = proc._process.poll()
                    if retval is not None:
                        log.debug(
                            "Returned a return value of {}".format(retval))
                        break
                    else:
                        time.sleep(0.5)
                else:
                    log.warning(
                        "Failed to terminate proc in alloted time")
                    log.info("Killing process")
                    proc._process.kill()
            self.state = "ready"
        else:
            log.error("pipleine state is not in state = running, nothing to stop")

    def deconfigure(self):
        """@brief deconfigure the dspsr pipeline."""
        self.state = "deconfiguring"
        log.debug("Destroying dada buffer")
        cmd = "dada_db -d -k {0}".format(self._dada_key)
        log.debug("Running command: {0}".format(cmd))
        self._destory_ring_buffer = ExecuteCommand(
            cmd, outpath=None, resident=False)
        self._destory_ring_buffer.stdout_callbacks.add(
            self._decode_capture_stdout)
        self._destory_ring_buffer._process.wait()
        self.state = "idle"


def main():
    logging.info("Starting Mkrecv2Db2Dspsr pipeline instance")
    server = Mkrecv2Db2Dspsr()
    server.configure()
    server.start()
    server.stop()
    server.deconfigure()

if __name__ == "__main__":
    main()
