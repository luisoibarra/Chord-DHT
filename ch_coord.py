import Pyro4 as pyro
import random
import logging as log
import sys
from ch_shared import *
import plac

@pyro.expose
@pyro.behavior(instance_mode='single')
class ChordCoordinator:
    
    ADDRESS = "coordinator.chord"
    
    def __init__(self, key_bits:int, dm_host:str, dm_port:int, ns_host:str, ns_port:int):
        self.node_addresses = {}
        self._daemon_host = dm_host
        self._daemon_port = dm_port
        self._name_server_host = ns_host
        self._name_server_port = ns_port
        self._bits = key_bits
        log.info(f"Started Coordinator with {key_bits} bits")
        print(key_bits)
        
    @property
    def bits(self):
        """
        Bit amount of the hash key
        """
        return self._bits
    
    @property
    def daemon_host(self):
        return self._daemon_host
    
    @property
    def daemon_port(self):
        return self._daemon_port
    
    @property
    def name_server_host(self):
        return self._name_server_host
    
    @property
    def name_server_port(self):
        return self._name_server_port
    
    
    @method_logger
    def register(self, node_id, address):
        """
        Register a Chord node.  
        
        node_id: Chord node id  
        address: Chord address  
        """
        log.info(f"Register node {node_id}: {address}")
        self.node_addresses[node_id] = address

    @method_logger
    def unregister(self, node_id):
        """
        Unregister a Chord node.
        """
        log.info(f"Unregister node {node_id}")
        self.node_addresses.__delitem__(node_id)
    
    @method_logger
    def get_initial_node(self):
        """
        Gets a random active node id from the registered nodes
        """
        if self.node_addresses:
            node_id, node_address = random.choice([x for x in self.node_addresses.items()])
            log.info(f"Returned initial node {node_id} with address {node_address}")
            return node_id
        log.info(f"No initial node found")
        return None

# plac annotation (description, type of arg [option, flag, positional], abrev, type, choices)
def main(bits:("Hash bits","option","b",int)=5,
         dm_host:("Pyro daemon host","option","ho",str)=None,
         dm_port:("Pyro daemon port","option","p",int)=0,
         ns_host:("Pyro name server host","option","nsh",str)=None,
         ns_port:("Pyro name server port","option","nsp",int)=None):
    log.basicConfig(level=log.DEBUG)
    with pyro.Daemon(dm_host, dm_port) as daemon:
        coord_dir = daemon.register(ChordCoordinator(bits, dm_host, dm_port, ns_host, ns_port))
        with pyro.locateNS(ns_host, ns_port) as ns:
            ns.register(ChordCoordinator.ADDRESS, coord_dir)
        daemon.requestLoop()
if __name__ == "__main__":
    plac.call(main)