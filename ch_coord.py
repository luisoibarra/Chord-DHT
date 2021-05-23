import Pyro4 as pyro
import random
import logging as log
import sys
from ch_shared import *

BITS = 3

@pyro.expose
@pyro.behavior(instance_mode='single')
class ChordCoordinator:
    
    ADDRESS = "coordinator.chord"
    
    def __init__(self, key_bits:int=BITS):
        self.node_addresses = {}
        self._bits = key_bits
        
    @property
    def bits(self):
        """
        Bit amount of the hash key
        """
        return self._bits
    
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

def main():
    
    log.basicConfig(level=log.DEBUG)
    pyro.Daemon.serveSimple({
        ChordCoordinator: ChordCoordinator.ADDRESS # Cuidado con la direccion
    })
    
if __name__ == "__main__":
    main()