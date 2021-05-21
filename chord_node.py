import Pyro4 as py
import Pyro4.util
import sys
sys.excepthook = Pyro4.util.excepthook

coordintor_dir = "PYRONAME:coordinator.chord" # Pyro remote coordinator dir

coordinator = py.Proxy(coordintor_dir) # Remote coordinator

# coordinator.join("TESTING") # RPC

p = coordinator.test
print(p)
p = coordinator.test
print(p)
coordinator.test = 10
p = coordinator.test
print(p)


@py.expose
class ChordNode:
    
    def __init__(self):
        self.id = None
        self.values = {}
        self.bits = None
        self.FT = [] # Finger table
        self.node_set = []
        self.node_dirs = {} # node_dirs[i] = node i direction
        self.max_nodes_amount = None
    
    @property
    def name(self):
        return f"node_{self.id}.chord"
    
    def start(self, coordinator):
        bits, node_id, nodes_dirs = coordinator.get_init_node_info()
        self.bits = bits
        self.FT = [None for _ in range(self.bits+1)] # FT[0] = predecessor
        self.id = node_id
        self.max_node_amount = 2**self.bits
        self.addNodes([x for x in nodes_dirs])
        self.recomputeFingerTable()
        self.node_dirs = nodes_dirs
        
    def inbetween(self, key, lwb, upb):
        if lwb <= upb:                   
            return lwb <= key and key < upb
        else:                             
            return (lwb <= key and key < upb + self.max_nodes_amount) or (lwb <= key + self.max_nodes_amount and key < upb)                    

    def addNodes(self, nodesID):
        self.node_set.extend(nodesID) 
        self.node_set = list(set(self.node_set))                                
        self.node_set.sort()              

    def delNode(self, nodeID):         
        assert nodeID in self.node_set, ''
        del self.node_set[self.node_set.index(nodeID)]                          
        self.node_set.sort()              

    def finger(self, i):
        succ = (self.id + pow(2, i-1)) % self.max_nodes_amount    # succ(p+2^(i-1))
        lwbi = self.node_set.index(self.id)               # own index in node_set
        upbi = (lwbi + 1) % len(self.node_set)                # index next neighbor
        for k in range(len(self.node_set)):                   # go through all segments
            if self.inbetween(succ, self.node_set[lwbi]+1, self.node_set[upbi]+1):
                return self.node_set[upbi]                        # found successor
            (lwbi,upbi) = (upbi, (upbi+1) % len(self.node_set)) # go to next segment
        return None                       

    def recomputeFingerTable(self):
        self.FT[0]  = self.node_set[self.node_set.index(self.id)-1] # Predecessor
        self.FT[1:] = [self.finger(i) for i in range(1,self.bits+1)] # Successors

    def localSuccNode(self, key): 
        if self.inbetween(key, self.FT[0]+1, self.id+1): # key in (FT[0],self]
            return self.id                                 # node is responsible
        elif self.inbetween(key, self.id+1, self.FT[1]): # key in (self,FT[1]]
            return self.FT[1]                                  # successor responsible
        for i in range(1, self.bits+1):                     # go through rest of FT
            if self.inbetween(key, self.FT[i], self.FT[(i+1) % self.bits]):
                return self.FT[i]                                # key in [FT[i],FT[i+1]) 

def main():
    node = ChordNode()
    node.start(coordinator)
    print(node.id)
    with py.Daemon() as daemon:
        node_uri = daemon.register(node)
        with py.locateNS() as ns:
            ns.register(node.name, node_uri)
        daemon.requestLoop()
    
if __name__ == "__main__":
    main()