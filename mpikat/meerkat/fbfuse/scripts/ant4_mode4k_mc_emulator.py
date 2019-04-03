import json
from optparse import OptionParser
from tornado.gen import coroutine
from tornado.ioloop import IOLoop
from katcp import KATCPClientResource

@coroutine
def setup_32beam_4ant(worker_addr, dc_addr):

    # Hardcoded numbers
    nbeams = 32
    tot_nchans = 4096
    feng_groups = "spead://239.8.0.0+3:7147"
    chan0_idx = 0
    chan0_freq = 1240e6
    chan_bw = 856e6 / tot_nchans

    dc_client = KATCPClientResource(dict(
        name="delay-configuration-client",
        address=dc_addr,
        controlled=True))
    yield dc_client.start()
    print "Syncing delay client"
    yield dc_client.until_synced(timeout=4.0)
    print "Synced"
    antennas_json = yield dc_client.sensor['antennas'].get_value()
    print "done"
    antennas = json.loads(antennas_json)
    print antennas
    worker_client = KATCPClientResource(dict(
        name="worker-server-client",
        address=worker_addr,
        controlled=True))
    yield worker_client.start()
    print "Syncing worker server"
    yield worker_client.until_synced()
    print "done"

    coherent_beams_csv = ",".join(["cfbf{:05d}".format(ii) for ii in range(nbeams)])
    feng_antenna_map = {antenna: ii for ii, antenna in enumerate(antennas)}
    coherent_beam_antennas = antennas
    incoherent_beam_antennas = antennas
    nantennas = len(antennas)
    nchans_per_group = tot_nchans / nantennas / 4
    mcast_to_beam_map = {
        "spead://239.11.1.0:7147": coherent_beams_csv,
        "spead://239.11.1.150:7147": "ifbf00001"
    }
    feng_config = {
        "bandwidth": 856e6,
        "centre-frequency": 1200e6,
        "sideband": "upper",
        "feng-antenna-map": feng_antenna_map,
        "sync-epoch": 12353524243.0,
        "nchans": 4096
    }
    coherent_beam_config = {
        "tscrunch": 16,
        "fscrunch": 1,
        "antennas": ",".join(coherent_beam_antennas)
    }
    incoherent_beam_config = {
        "tscrunch": 16,
        "fscrunch": 1,
        "antennas": ",".join(incoherent_beam_antennas)
    }

    print "Making prepare request"
    response = yield worker_client.req.prepare(
        feng_groups, nchans_per_group, chan0_idx, chan0_freq,
        chan_bw, nbeams, json.dumps(mcast_to_beam_map),
        json.dumps(feng_config),
        json.dumps(coherent_beam_config),
        json.dumps(incoherent_beam_config),
        *dc_addr)
    if not response.reply.reply_ok():
        raise Exception("Error on prepare: {}".format(response.reply.arguments))
    else:
        print "prepare done"


if __name__ == "__main__":
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('', '--dc-ip', dest='dc_ip', type=str,
                      help='Host interface to bind to')
    parser.add_option('', '--dc-port', dest='dc_port', type=int,
                      help='Port number to bind to')
    parser.add_option('', '--worker-ip', dest='worker_ip', type=str,
                      help='Host interface to bind to')
    parser.add_option('', '--worker-port', dest='worker_port', type=int,
                      help='Port number to bind to')
    (opts, args) = parser.parse_args()
    ioloop = IOLoop.current()
    ioloop.run_sync(
        lambda: setup_32beam_4ant(
            (opts.worker_ip, opts.worker_port),
            (opts.dc_ip, opts.dc_port)))

