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

class BlockingRequest(BlockingClient):
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
                #for msg in reply:
                #        log.info(self.decode_katcp_message(msg.__str__()))
                for msg in informs:
                        log.info(self.decode_katcp_message(msg.__str__()))

        def start(self):
                self.setDaemon(True)
                super(BlockingRequest, self).start()
                self.wait_protocol()

        def stop(self):
                super(BlockingRequest, self).stop()
                super(BlockingRequest, self).join()

        def help(self):
                reply, informs = self.blocking_request(katcp.Message.request("help"), timeout=20)
                self.to_stream(reply, informs)

#        def configure(self, paras, sensors):
#                reply, informs = self.blocking_request(katcp.Message.request("configure", paras, sensors))
#                self.to_stream(reply, informs)

        def configure(self, paras):
                reply, informs = self.blocking_request(katcp.Message.request("configure", paras))
                self.to_stream(reply, informs)
		return reply

        def deconfigure(self):
                reply, informs = self.blocking_request(katcp.Message.request("deconfigure"))
                self.to_stream(reply, informs)
                return reply

        def capture_start(self):
                reply, informs = self.blocking_request(katcp.Message.request("start"))
                self.to_stream(reply, informs)
                return reply

        def capture_stop(self):
                reply, informs = self.blocking_request(katcp.Message.request("stop"))
                self.to_stream(reply, informs)
                return reply
