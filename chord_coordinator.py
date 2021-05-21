import zmq
import Pyro4 as py
import random

bits = 5
max_nodes = 5

@py.expose
@py.behavior(instance_mode='single')
class ChordCoordinator:
    
    def __init__(self):
        self.bits = bits
        self.max_nodes = max_nodes
        self.current_nodes = 0
        self.node_addresses = {}
        
    def _get_new_id(self):
        if self.max_nodes > self.current_nodes:
            new_id = 0
            while new_id in self.node_addresses:
                new_id = random.randint(0, 2**self.bits - 1)
            return new_id
        raise Exception("Max number of nodes reached")
        
    def get_init_node_info(self):
        """
        When a node starts call this
        """
        new_id = self._get_new_id()
        self.node_addresses[new_id] = "dummy_address"
        return self.bits, new_id, self.node_addresses

    
    def get_test(self):
        self.max_nodes+=1
        return self.max_nodes
            
    def set_test(self, value):
        self.max_nodes = value

    test = property(get_test,set_test)

def main():
    py.Daemon.serveSimple({
        ChordCoordinator: "coordinator.chord" # Cuidado con la direccion
    })
    
if __name__ == "__main__":
    main()