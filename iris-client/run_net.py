from mininet.log import setLogLevel, info
# from mn_wifi.link import wmediumd, adhoc
# from mn_wifi.cli import CLI_wifi
from mininet.cli import CLI
from mininet.net import Mininet
# from mininet.link import TCLink
# from mn_wifi.topo import Topo_WiFi
# from mn_wifi.link import wmediumd, adhoc
# from mn_wifi.wmediumdConnector import interference
from mininet.node import RemoteController
from mininet.link import TCLink
import net

def topology():
    "Create a network."
    net = Mininet()
    # ctrl = net.addController('c0', controller=RemoteController, ip="172.17.0.2", port=6653)
    bw = 100

    info("*** Creating nodes\n")
    cn1 = net.addHost("cn1")
    cn2 = net.addHost("cn2")

    info("*** Creating switch")
    fw1 = net.addSwitch("fw1",protocols=["OpenFlow14"],failMode="standalone")

    info("*** Creating links\n")
    # net.addLink(fw1, cn1)
    # net.addLink(fw1, cn2)
    net.addLink(fw1, cn1, cls=TCLink, delay='4ms',use_htb=True)
    net.addLink(fw1, cn2, cls=TCLink, delay='4ms',use_htb=True)

    info("*** Starting network\n")
    net.build()
    fw1.start([])

    info("*** Start iris server\n")
    log = open(f'{cn1.IP()}.log', 'w')
    p1 = cn1.popen(["/home/skye/iris/iris-client/target/release/server","-l",cn1.IP(),"-p","12345"],stdout=log,stderr=log)
    log = open(f'{cn2.IP()}.log', 'w')
    p2 = cn2.popen(["/home/skye/iris/iris-client/target/release/server","-l",cn2.IP(),"-p","12345"],stdout=log,stderr=log)

    info("*** Running CLI\n")
    CLI(net)

    p1.terminate()
    p2.terminate()
    info("*** Stopping network\n")
    net.stop()


if __name__ == '__main__':
    setLogLevel('debug')
    topology()
