import os
import socket
import fcntl
import struct
import pynvml
import logging
__numaInfo = None


def updateInfo():
    """
    Updates the info dictionary.
    """
    logging.debug("Update numa dictionary")
    global __numaInfo
    __numaInfo = {}
    nodes = open("/sys/devices/system/node/possible").read().strip().split('-')

    for node in nodes:
        logging.debug("Preparing node {} of {}".format(node, len(nodes)))
        __numaInfo[node] = {"net_devices":{} }

        cpurange = open('/sys/devices/system/node/node' + node + '/cpulist').read().strip().split('-')
        __numaInfo[node]['cores'] = map(str, range(int(cpurange[0]), int(cpurange[1])+1))
        __numaInfo[node]['gpus'] = []
        __numaInfo[node]["net_devices"] = {}
        logging.debug("  found {} Cores.".format(len(__numaInfo[node]['cores'])))

    logging.debug(__numaInfo)
    # check network devices
    for device in os.listdir("/sys/class/net/"):
        logging.debug("Associate network device {} to node".format(device))
        d = "/sys/class/net/" + device + "/device/numa_node"
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if os.path.isfile(d):
            node = open(d).read().strip()
            __numaInfo[node]["net_devices"][device] = {}
            logging.debug("  - found node {}".format(node))
            __numaInfo[node]["net_devices"][device]['ip'] = ""
            try:
                ip = socket.inet_ntoa(fcntl.ioctl(s.fileno(),
                    0x8915,  # SIOCGIFADDR
                    struct.pack('256s', device[:15]))[20:24])
                __numaInfo[node]["net_devices"][device]['ip'] = ip
            except IOError as e:
                logging.warning(" Cannot associate device {} to a node: {}".format(device, e))

            d = "/sys/class/net/" + device + "/speed"
            speed = 0
            if os.path.isfile(d):
                try:
                    speed = open(d).read()
                except:
                    logging.warning(" Cannot acess speed for device {}: {}".format(device, e))

            __numaInfo[node]["net_devices"][device]['speed'] = int(speed)

    # check cuda devices:
    pynvml.nvmlInit()

    nGpus = pynvml.nvmlDeviceGetCount()

    for i in range(nGpus):
        handle = pynvml.nvmlDeviceGetHandleByIndex(i)
        pciInfo = pynvml.nvmlDeviceGetPciInfo(handle)

        d = '/sys/bus/pci/devices/' + pciInfo.busId + "/numa_node"
        node = open(d).read().strip()
        __numaInfo[node]['gpus'].append(str(i))


def getInfo():
    """
    Returns dict with info on numa configuration. For every numa node the dict
    contains a dict with the associated ressources.
    """
    global __numaInfo
    if not __numaInfo:
        updateInfo()
    return __numaInfo


def getFastestNic(numa_node):
    """
    Returns (name, description) of the fastest nic on given numa_node
    """
    nics = getInfo()[numa_node]["net_devices"]
    fastest_nic = max(nics.iterkeys(), key=lambda k: nics[k]['speed'])
    return fastest_nic, nics[fastest_nic]


if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    print getInfo()
    for node, res in getInfo().items():
        print("NUMA Node: {}".format(node))
        print("  CPU Cores: {}".format(", ".join(res['cores'])))
        print("  GPUs: {}".format(", ".join(map(str, res['gpus']))))
        print("  Network interfaces:")
        for nic, info in res['net_devices'].items():
            print("     {nic}: ip = {ip}, speed = {speed} Mbit/s ".format(nic=nic, **info))

        nics = res['net_devices']
        if len(nics) > 0:
            fastest_nic = max(nics.iterkeys(), key=lambda k: nics[k]['speed'])
            print('   -> Fastest interface: {}'.format(fastest_nic))
