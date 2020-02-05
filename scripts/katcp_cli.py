#!/usr/bin/env python
import logging
import sys
import traceback
import katcp
import readline
import codecs
import re
from optparse import OptionParser
from cmd2 import Cmd
from katcp import DeviceClient

logging.basicConfig(level=logging.INFO,
                    stream=sys.stderr,
                    format="%(asctime)s - %(name)s - %(filename)s:"
                    "%(lineno)s - %(levelname)s - %(message)s")

log = logging.getLogger("r2rm.basic_cli")

ESCAPE_SEQUENCE_RE = re.compile(r'''
    ( \\U........      # 8-digit hex escapes
    | \\u....          # 4-digit hex escapes
    | \\x..            # 2-digit hex escapes
    | \\[0-7]{1,3}     # Octal escapes
    | \\N\{[^}]+\}     # Unicode characters by name
    | \\[\\'"abfnrtv]  # Single-character escapes
    )''', re.UNICODE | re.VERBOSE)

def unescape_string(s):
    def decode_match(match):
        return codecs.decode(match.group(0), 'unicode-escape')
    return ESCAPE_SEQUENCE_RE.sub(decode_match, s)


def decode_katcp_message(s):
    """
    @brief      Render a katcp message human readable

    @params s   A string katcp message
    """
    return unescape_string(s).replace("\_", " ")

class StreamClient(DeviceClient):
    def __init__(self, server_host, server_port, stream=sys.stdout):
        self.stream = stream
        super(StreamClient, self).__init__(server_host, server_port)
        self.commands = set()

    def inform_help(self, msg):
        """ Handls inform"""
        self.unhandled_inform(msg)
        self.commands.add(msg.arguments[0])

    def to_stream(self, prefix, msg):
        self.stream.write("%s:\n%s\n" %
                          (prefix, decode_katcp_message(msg.__str__())))

    def unhandled_reply(self, msg):
        """Deal with unhandled replies"""
        self.to_stream("Unhandled reply", msg)

    def unhandled_inform(self, msg):
        """Deal with unhandled replies"""
        self.to_stream("Unhandled inform", msg)

    def unhandled_request(self, msg):
        """Deal with unhandled replies"""
        self.to_stream("Unhandled request", msg)



class KatcpCli(Cmd):
    """
    @brief      Basic command line interface to KATCP device

    @detail     This class provides a command line interface to
                to any katcp.DeviceClient subclass. Behaviour of the
                interface is determined by the client object passed
                at instantiation.
    """
    Cmd.shortcuts.update({'?': 'katcp'})
    Cmd.allow_cli_args = False
    def __init__(self,host,port,*args,**kwargs):
        """
        @brief  Instantiate new KatcpCli instance

        @params client A DeviceClient instance
        """
        self.host = host
        self.port = port
        self.katcp_parser = katcp.MessageParser()
        self.start_client()
        Cmd.__init__(self, *args, persistent_history_file='~/.katcp_cli_history', **kwargs)

    def start_client(self):
        log.info("Client connecting to port {self.host}:{self.port}".format(**locals()))
        self.client = StreamClient(self.host, self.port)
        self.client.start()
        self.prompt = "(katcp CLI {self.host}:{self.port}): ".format(**locals())

    def stop_client(self):
        self.client.stop()
        self.client.join()

    def do_katcp(self, arg, opts=None):
        """
        @brief      Send a request message to a katcp server

        @param      arg   The katcp formatted request
        """
        request = "?" + "".join(arg)
        log.info("Request: %s"%request)
        try:
            msg = self.katcp_parser.parse(request)
            self.client.ioloop.add_callback(self.client.send_message, msg)
        except Exception, e:
            e_type, e_value, trace = sys.exc_info()
            reason = "\n".join(traceback.format_exception(
                e_type, e_value, trace, 20))
            log.exception(reason)

    def complete_katcp(self, text, line, begidx, endidx):
        """
        Tab completion for katcp requests.
        """
        if not self.client.commands:
            # ToDo: this request should be silent and not be printed on the cli
            # ...
            self.client.ioloop.add_callback(self.client.send_message, "?help")

        return self.basic_complete(text, line, begidx, endidx, self.client.commands)

    def do_connect(self, arg, opts=None):
        """
        @brief      Connect to different KATCP server

        @param      arg   Target server address in form "host:port"
        """
        try:
            host,port = arg.split(":")
        except Exception:
            print "Usage: connect <host>:<port>"
            return
        try:
            app = KatcpCli(host,port)
            app.cmdloop()
        except Exception as error:
            log.exception("Error from CLI")
        finally:
            app.stop_client()


if __name__ == "__main__":
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-a', '--host', dest='host', type="string", default="", metavar='HOST',
                      help='attach to server HOST (default="" - localhost)')
    parser.add_option('-p', '--port', dest='port', type=int, default=1235, metavar='N',
                      help='attach to server port N (default=1235)')
    (opts, args) = parser.parse_args()
    sys.argv = sys.argv[:1]
    log.info("Ctrl-C to terminate.")
    try:
        app = KatcpCli(opts.host,opts.port)
        app.cmdloop()
    except Exception as error:
        log.exception("Error from CLI")
    finally:
        app.stop_client()
