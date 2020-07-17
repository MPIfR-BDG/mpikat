import codecs
import re
import sys
import katcp
import logging
from katcp import BlockingClient, Message

logging.basicConfig(level=logging.INFO,
                    stream=sys.stderr,
                    format="%(asctime)s - %(name)s - %(filename)s:"
                    "%(lineno)s - %(levelname)s - %(message)s")

log = logging.getLogger("blockingclient")

ESCAPE_SEQUENCE_RE = re.compile(r'''
    ( \\U........      # 8-digit hex escapes
    | \\u....          # 4-digit hex escapes
    | \\x..            # 2-digit hex escapes
    | \\[0-7]{1,3}     # Octal escapes
    | \\N\{[^}]+\}     # Unicode characters by name
    | \\[\\'"abfnrtv]  # Single-character escapes
    )''', re.UNICODE | re.VERBOSE)

# copied from Tobi's GatedSpectrometerPipeline.py on dev branch
DEFAULT_CONFIG = {
        # default cfgs for master controler. Needs to get a unique ID -- TODO,
        # from ansible
        "id": "GatedSpectrometer",
        "type": "GatedSpectrometer",
        # supproted input formats name:version
        "supported_input_formats": {"MPIFR_EDD_Packetizer": [1]},
        # 256 Mega sampels per buffer block to allow high res  spectra - the
        "samples_per_block": 256 * 1024 * 1024,
                                                            # theoretical  mazimum is thus  128 M Channels.   This option  allows
                                                            # to tweak  the execution on  low-mem GPUs or  ig the GPU is  shared
                                                            # with other  codes
        "input_data_streams":
        {
            "polarization_0":
            {
                # name of the source for automatic setting of paramters
                "source": "",
                "description": "",
                # Format has version seperated via colon
                "format": "MPIFR_EDD_Packetizer:1",
                "ip": "225.0.0.152+3",
                "port": "7148",
                "bit_depth": 8,
                "sample_rate": 3200000000,
                "sync_time": 1581164788.0,
                # this needs to be consistent with the mkrecv configuration
                "samples_per_heap": 4096,
            },
             "polarization_1":
            {
                # name of the source for automatic setting of paramters, e.g.:
                # "packetizer1:h_polarization
                "source": "",
                "description": "",
                "format": "MPIFR_EDD_Packetizer:1",
                "ip": "225.0.0.156+3",
                "port": "7148",
                "bit_depth": 8,
                "sample_rate": 3200000000,
                "sync_time": 1581164788.0,
                # this needs to be consistent with the mkrecv configuration
                "samples_per_heap": 4096,
            }
        },
        "output_data_streams":
        {
            "polarization_0_0":
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.0.172",  # two destinations gate on/off
                "port": "7152",
            },
            "polarization_0_1":
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.0.173",  # two destinations gate on/off
                "port": "7152",
            },
             "polarization_1_0":
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.0.174",  # two destinations, one for on, one for off
                "port": "7152",
            },
             "polarization_1_1":
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.0.175",  # two destinations, one for on, one for off
                "port": "7152",
            }
        },

        "fft_length": 1024 * 1024 * 2 * 8,
        "naccumulate": 32,
        "output_bit_depth": 32,

        "input_level": 100,
        "output_level": 100,

        # ToDo: Should be a output data stream def.
        "output_directory": "/mnt",
        # ['network', 'disk', 'null']  ToDo: Should be a output data stream def.
        "output_type": 'network',
        # Use dummy input instead of mkrecv process. Should be input data
        # stream option.
        "dummy_input": False,
        "log_level": "debug",

        # True output date rate is multiplied by this factor for sending.
        "output_rate_factor": 1.10,
        "idx1_modulo": "auto",
    }


class BlockingRequest(BlockingClient):
                """
                @brief      Blocking request class for KATCP server

                """
        def __init__(self, host, port):
                device_host = host
                device_port = port
                super(BlockingRequest, self).__init__(device_host, device_port)

        def __del__(self):
                super(BlockingRequest, self).stop()
                super(BlockingRequest, self).join()

        def unescape_string(self, s):
                def decode_match(match):
                        return codecs.decode(match.group(0), 'unicode-escape')
                return ESCAPE_SEQUENCE_RE.sub(decode_match, s)

        def decode_katcp_message(self, s):
                """
                @brief      Render a katcp message human readable

                @params s   A string katcp message
                """
                return self.unescape_string(s).replace("\_", " ")

        def to_stream(self, reply, informs):
                log.info(self.decode_katcp_message(reply.__str__()))
                # for msg in reply:
                #        log.info(self.decode_katcp_message(msg.__str__()))
                for msg in informs:
                        log.info(self.decode_katcp_message(msg.__str__()))

        def start(self):
                """
                @brief      Start the blocking client

                """
                self.setDaemon(True)
                super(BlockingRequest, self).start()
                self.wait_protocol()

        def stop(self):
                """
                @brief      Stop the blocking client

                """
                super(BlockingRequest, self).stop()
                super(BlockingRequest, self).join()

        def help(self):
                """
                @brief      Send help command to the server

                """
                reply, informs = self.blocking_request(katcp.Message.request("help"), timeout=20)
                self.to_stream(reply, informs)

        def configure(self, config):
                """
                @brief      Send configure command to the server
                @config     configuration keyword 

                """
                # at the moment only the deafult config will get pass to the server so 
                if config == "GatedSpectrometer":
                	json_config = json.dumps(DEFAULT_CONFIG)
                reply, informs = self.blocking_request(katcp.Message.request("configure", json_config))
                self.to_stream(reply, informs)
                return reply

        def deconfigure(self):
                """
                @brief      Send deconfigure command to the server

                """
                reply, informs = self.blocking_request(katcp.Message.request("deconfigure"))
                self.to_stream(reply, informs)
                return reply

        def measurement_prepare(self, paras):
                """
                @brief      Send measurement_prepare command to the server
                @paras      Parameters to pass to the server

                """
                reply, informs = self.blocking_request(katcp.Message.request("measurement-prepare", paras))
                self.to_stream(reply, informs)
                return reply

        def measurement_start(self):
                """
                @brief      Send measurement_start command to the server

                """
                reply, informs = self.blocking_request(katcp.Message.request("measurement-start"))
                self.to_stream(reply, informs)
                return reply

        def measurement_stop(self):
                """
                @brief      Send measurement_stop command to the server

                """
                reply, informs = self.blocking_request(katcp.Message.request("measurement-stop"))
                self.to_stream(reply, informs)
                return reply
