import json
import igui_sidecar as s
import signal
import logging
import tornado

def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--host', dest='host', type=str,
                      help='The hostname for the KATCP server to connect to')
    parser.add_option('-p', '--port', dest='port', type=long,
                      help='The port number for the KATCP server to connect to')
    parser.add_option('', '--igui_host', dest='igui_host', type=str,
                      help='The hostname of the iGUI interface', default="127.0.0.1")
    parser.add_option('', '--igui_user', dest='igui_user', type=str,
                      help='The username for the iGUI connection')
    parser.add_option('', '--igui_pass', dest='igui_pass', type=str,
                      help='The password for the IGUI connection')
    parser.add_option('', '--igui_rx_id', dest='igui_rx_id', type=str,
                      help='The iGUI receiver ID for the managed device')
    parser.add_option('', '--igui_nodename', dest='igui_nodename', type=str,
                      help='The nodename for the managed device')
    parser.add_option('', '--igui_numa', dest='igui_numa', type=str,
                      help='The numa number for the managed device')
    parser.add_option('', '--log_level', dest='log_level', type=str,
                      help='Logging level', default="INFO")

    (opts, args) = parser.parse_args()
    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logger = logging.getLogger('mpikat')
    logging.basicConfig(format=FORMAT)
    logger.setLevel(opts.log_level.upper())
    logging.getLogger('katcp').setLevel('INFO')
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting KATCPToIGUIConverter instance")
    
    x =s.IGUIConnection(opts.igui_host, opts.igui_user, opts.igui_pass)
    x.login()
    icom_id="5f137055751811e8991a0242ac120002"
    y=x.build_igui_representation()
    rx_id = opts.igui_rx_id
    #rx_id = "8880b3e7d92711e8902a0242ac130002"
    rx=y.by_id(rx_id)
    #print rx.id
    hostname = "fbfnn00"
    numa = 0
    device_name = opts.igui_nodename+"_worker_"+opts.numa

    try:
        print "looking for device named []".format(device_name)
        y.by_id(rx_id).devices.by_name(device_name)
    except KeyError as error:
        print "device not found, let's add a device"
        paras = (device_name, "None", "N")
        result = json.loads(x.create_device(rx, paras))
        igui_device_id = result[0]["device_id"]
        print result[0]["device_id"]
    client = KATCPToIGUIConverter(opts.host, opts.port,
                                  opts.igui_host, opts.igui_user,
                                  opts.igui_pass, igui_device_id)
    
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, client))

    def start_and_display():
        client.start()
        log.info("Ctrl-C to terminate client")

    ioloop.add_callback(start_and_display)
    ioloop.start()
    
if __name__ == "__main__":
    main()
