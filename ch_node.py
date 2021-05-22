import Pyro4 as pyro
import Pyro4.util
from concurrent.futures import ThreadPoolExecutor, Future
from ch_coord import ChordCoordinator
import sys
import logging as log
from ch_shared import *
sys.excepthook = Pyro4.util.excepthook


class FingerTableEntry:
    
    def __init__(self, node_key:int, entry_bit:int, total_bits:int, successor:int=None):
        if entry_bit == 0: # Predecessor
            entry_bit = 1 # To avoid negative shift

        self.start = (node_key + (1 << (entry_bit - 1))) % (1 << total_bits)
        self.end = (self.start + (1 << (entry_bit - 1))) % (1 << total_bits)
        self.successor = successor

    def __str__(self):
        return f"{self.start} - {self.end}   {self.successor}"

    def __repr__(self):
        return str(self)

@pyro.expose
class ChordNode:
    
    CHORD_NODE_PREFIX = "chord.node."
    
    @staticmethod
    def hash(key):
        """
        Hash function used by ChordNode
        """
        return hash(key)
    
    @staticmethod
    def node_name(id):
        """
        Naming convention for ChordNode
        """
        return f"{ChordNode.CHORD_NODE_PREFIX}{id}"

    @property
    def successor(self):
        return self.finger_table[1].successor
    
    def _get_predecessor(self):
        return self.finger_table[0].successor
    
    def _set_predecessor(self, value):
        self.finger_table[0].successor = value

    predecessor = property(_get_predecessor, _set_predecessor)
    
    def _get_id(self):
        return self._id
    
    def _set_id(self, value):
        """
        Set node id.  
        Id can't change once setted. 
        """
        if self._id == None:
            self._id = value
    
    id = property(_get_id, _set_id)
    
    def __init__(self, forced_id=None):
        self.listeners = []
        self._id = None
        self.id = forced_id
        self.bits = None
        self.running = False
        self.executor = ThreadPoolExecutor()

    def lookup(self, key):
        """
        yields the IP of the node responsible for holding the key value 
        """
        pass
    
    def register_listener(self, listener):
        """
        register an object that listen when the nodes keys changed due to relocation
        """
        self.listeners.append(listener)
    
    def cli_loop(self):
        """
        Command Line Interface to talk with ChordNode
        """
        command = None
        while True:
            command = input()
            if command == "ft":
                print(self.finger_table)
            elif command == "id":
                print(self.id)
            elif command == "exit":
                self.leave()
                break
            else:
                print("Invalid command")
    
    def leave(self):
        """
        Leave DHT table
        """
        pass
    
    @method_logger
    def start(self, coordinator_address):
        """
        Start the node taking initial_node as reference to fill the finger_table. 
        In case initial_node is None the the current node is the first in the DHT. 
        """
        self.running = True
        
        self.executor.submit(self.cli_loop)
        
        with pyro.Daemon() as daemon:
            self.daemon = daemon
            
            # Setting up node
            coordinator = create_object_proxy(coordinator_address)
            self.bits = coordinator.bits
            
            # Getting initial node
            initial_node_id = coordinator.get_initial_node()
            if initial_node_id != None:
                initial_node = self.get_node_proxy(initial_node_id)
            else:
                initial_node = None
            
            # Register node in pyro name server and deamon
            self.dir = daemon.register(self)
            self.id = ChordNode.hash(self.dir) % (1 << self.bits)
            with pyro.locateNS() as ns:
                ns.register(ChordNode.node_name(self.id), self.dir)
            
            # Joining DHT
            self.join(initial_node)

            daemon.requestLoop()
            self.daemon = None
        
        self.running = False
    
    def in_between(self, key, lwb, upb):
        """
        Checks if key is between lwb and upb with modulus 2**bits
        """
        max_nodes = 1 << self.bits
        if lwb == upb:
            return True
        elif lwb < upb:                   
            return lwb <= key and key < upb
        else:                             
            return (lwb <= key and key < upb + max_nodes) or (lwb <= key + max_nodes and key < upb)                    
   
    @method_logger
    def find_successor(self, key):
        """
        Finds and returns the node's id for key successor
        """
        pred_id = self.find_predecessor(key)
        pred_node = self.get_node_proxy(pred_id)
        return pred_node.successor
    
    @method_logger
    def find_predecessor(self, key):
        """
        Finds and returns the node's id for key predecessor
        """
        current = self
        
        while not (self.in_between(key, current.id + 1, current.successor + 1)):
            current_id = current.closest_preceding_finger(key)
            current = self.get_node_proxy(current_id)
            #QUITAR
            print(key, current.id, current.successor)
        return current.id
    
    def closest_preceding_finger(self, key):
        """
        Return the closest preceding finger node's id from key
        """
        for i in range(self.bits,0,-1):
            if self.in_between(self.finger_table[i].successor, self.id + 1, key):
                return self.finger_table[i].successor
        return self.id
    
    @method_logger
    def register(self):
        """
        Register current node in DHT coordinator
        """
        coordinator = create_object_proxy(ChordCoordinator.ADDRESS)
        coordinator.register(self.id, self.dir)
            
    @method_logger
    def join(self, initial_node):
        """
        Do the initialization of the node using initial_node. 
        If initial_node is None then the current node is the first in the DHT 
        """

        self.register()
        
        if initial_node is None:
            # All finger_table entries are self
            self.finger_table = [FingerTableEntry(self.id, i, self.bits, self.id) for i in range(self.bits+1)]
        else:
            self.init_finger_table(initial_node)
            self.executor.submit(self.update_others) # Let the current node accept RPC from now on
            # self.update_others()
    
    @method_logger
    def init_finger_table(self, initial_node):
        """
        Fill the node's finger_table using initial_node.
        """
        self.finger_table = [FingerTableEntry(self.id, i, self.bits, None) for i in range(self.bits+1)]
        
        successor_id = self.finger_table[1].successor = initial_node.find_successor(self.finger_table[1].start)
        successor_node = self.get_node_proxy(successor_id)
        # Update predecessors
        self.predecessor = successor_node.predecessor
        successor_node.predecessor = self.id
        # Update finger table
        for i in range(1, self.bits):
            upper_entry = self.finger_table[i+1]
            current_entry = self.finger_table[i]
            if self.in_between(upper_entry.start, self.id, current_entry.successor):
                upper_entry.successor = current_entry.successor
            else:
                upper_entry.successor = initial_node.find_successor(upper_entry.start)
    
    @method_logger
    def update_others(self):
        """
        Update finger tables of nodes that should include this node
        """
        for i in range(1, self.bits + 1):
            pred_id = self.find_predecessor((self.id - (1 << (i-1))) % (1 << self.bits))
            pred_node = self.get_node_proxy(pred_id)
            pred_node.update_finger_table(self.id, i)
            #QUITAR
            print("Updated with", self.id, i)
                
    @method_logger
    def update_finger_table(self, s, i):
        """
        Updates figer table at i if s is better suited
        """
        if self.id == s:
            return
        
        if self.in_between(s, self.id, self.finger_table[i].successor):
            self.finger_table[i].successor = s
            pred_node = self.get_node_proxy(self.predecessor)
            pred_node.update_finger_table(s, i)
                
    def get_node_proxy(self, id):
        """
        Returns a Chord Node proxy for the given id
        """
        if id != self.id:
            node = create_object_proxy(ChordNode.node_name(id))
        else:
            node = self
        return node