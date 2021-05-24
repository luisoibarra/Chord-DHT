import Pyro4 as pyro
import Pyro4.util
from concurrent.futures import ThreadPoolExecutor, Future
from ch_coord import ChordCoordinator
import sys
import logging as log
from ch_shared import *
import time
import random
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
    
    def hash(self, value):
        """
        Hash function used by ChordNode
        """
        return hash(value) % (1 << self.bits) 
    
    @staticmethod
    def node_name(id):
        """
        Naming convention for ChordNode
        """
        return f"{ChordNode.CHORD_NODE_PREFIX}{id}"

    def _get_successor(self):
        return self.finger_table[1].successor
    
    def _set_successor(self, value):
        self.add_successor_list(value)
        self.finger_table[1].successor = value

    successor = property(_get_successor, _set_successor)

    
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
    
    def __init__(self, forced_id=None, stabilization=False):
        self.listeners = []
        self._id = None
        self.id = forced_id
        self.bits = None
        self.successor_list = []
        self.stabilization = stabilization
        self.running = False
        self.values = {}
        self.executor = ThreadPoolExecutor()

    @method_logger
    def lookup(self, key):
        """
        Returns the value associated with the key 
        """
        key = self.hash(key)
        if self.in_between(key, self.predecessor + 1, self.id + 1):
            return self.values[key]
        successor_id = self.find_successor(key)
        successor = self.get_node_proxy(successor_id)
        return successor.lookup(key)
    
    @method_logger
    def insert(self, value, key:int=None):
        """
        Insert value into the DHT. If key is given then it will be inserted with it.
        """
        if key == None:
            key = self.hash(value)
        successor_id = self.find_successor(key)
        if successor_id == self.id:
            self.values[key] = value
        else:
            successor = self.get_node_proxy(successor_id)
            successor.insert(value, key)
    
    @method_logger
    def register_listener(self, listener):
        """
        register an object that listen when the nodes keys changed due to relocation.  
        The listeners must have a key_relocated method that receives a list of integer, indicating   
        the keys that were changed.
        """
        self.listeners.append(listener)
    
    @method_logger
    def notify_listeners(self, keys:list):
        """
        Notify listeners that keys were relocated
        """
        for l in self.listeners:
            l.keys_relocated(keys)
    
    
    def cli_loop(self):
        """
        Command Line Interface to talk with ChordNode
        """
        command = None
        help_msg="ft: print finger table\nid: print node id\nkeys: print local key:value\n exit: shutdown chord node"
        while True:
            command = input()
            if command == "ft":
                print(self.finger_table)
            elif command == "id":
                print(self.id)
            elif command == "keys":
                print("\n".join([f"- {x}:{self.values[x]}" for x in self.values]))
            elif command == "exit":
                self.leave()
                break
            else:
                print("Invalid command:\n", help_msg)
    
    @method_logger
    def leave(self):
        """
        Leave DHT table
        """
        try:
            # Update predecessor and successor and transfer the current keys
            successor_node = self.get_node_proxy(self.successor)
            predecessor_node = self.get_node_proxy(self.predecessor)
            successor_node.predecessor = self.predecessor
            predecessor_node.successor = self.successor
            successor_node.update_values(self.values)
            if not self.stabilization:
                successor_node.update_leaving_node(self.id, successor_node.id)
            else:
                # Stabilization 
                pass
        except Exception as exc:
            log.exception(exc)
        
        try:
            coordinator = create_object_proxy(self.coordinator_address)
            coordinator.unregister(self.id)
        except Exception as exc:
            log.exception(exc)
            
        self.daemon.shutdown()
    
    def update_leaving_node(self, node_id, new_successor_id):
        """
        Update finger table having node_id with successor_id
        """
        for i in range(1, self.bits + 1):
            finger_table_entry = self.finger_table[i]
            if finger_table_entry.successor == node_id:
                finger_table_entry.successor = new_successor_id
            elif self.in_between(finger_table_entry.successor, self.id, node_id):
                successor_id = self.find_successor(finger_table_entry.successor)
                successor_node = self.get_node_proxy(successor_id)
                successor_node.update_leaving_node(node_id, new_successor_id)
    
    @method_logger
    def update_values(self, new_values:dict):
        """
        Update the values dictionary with new_values
        """
        for x in new_values:
            if self.in_between(x, self.predecessor + 1, self.id + 1):
                self.values[x] = new_values[x]
        
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
            self.coordinator_address = coordinator_address
            self.bits = coordinator.bits
            self.max_successor_list_count = self.bits
            
            # Getting initial node
            initial_node_id = coordinator.get_initial_node()
            if initial_node_id != None:
                initial_node = self.get_node_proxy(initial_node_id)
            else:
                initial_node = None
            
            # Register node in pyro name server and deamon
            self.dir = daemon.register(self)
            self.id = self.hash(self.dir)
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
        try: 
            return self.get_node_proxy(pred_node.successor).id
        except:
            return pred_node.id
    
    @method_logger
    def find_predecessor(self, key):
        """
        Finds and returns the node's id for key predecessor
        """
        current = self
        
        while not (self.in_between(key, current.id + 1, current.successor + 1)):
            current_id = current.closest_preceding_finger(key)
            try:
                current = self.get_node_proxy(current_id)
            except:
                current = self.successor_node_from_succesor_list(key)
                return current.find_predecessor(key)
            log.info(f"find_predecessor cycle: key:{key} current_id:{current.id}, current_successor:{current.successor}")
        return current.id
    
    def closest_preceding_finger(self, key):
        """
        Return the closest preceding finger node's id from key
        """
        for i in range(self.bits,0,-1):
            if self.finger_table[i].successor != None and self.in_between(self.finger_table[i].successor, self.id + 1, key):
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
            # All finger_table entries are None
            self.finger_table = [FingerTableEntry(self.id, i, self.bits, None) for i in range(self.bits+1)]
            
        if not self.stabilization and initial_node != None:
            # Full finger table initialization, doesn't do stabilization
            self.init_finger_table(initial_node)
            self.executor.submit(self.init_node_last_part) # Let the current node accept RPC from now on
        elif self.stabilization:
            # More loose table initialization complemented with periodic calls to maintain the table
            if initial_node != None:
                self.successor = initial_node.find_successor(self.id)
            self.executor.submit(self.stabilize_loop)
            self.executor.submit(self.fix_fingers_loop)
    
    @method_logger
    def stabilize_loop(self):
        """
        Periodically calls stabilize method
        """
        interval_milliseconds = 2000
        while self.running:
            try:
                self.stabilize()
            except Exception as exc:
                log.exception(exc)
            time.sleep(interval_milliseconds/1000)
    
    @method_logger
    def stabilize(self):
        """
        Verifies current node's immediate successor and notifies it about current node's existence
        """
        old_successor_id = self.find_successor(self.successor)
        old_successor_node = self.get_node_proxy(old_successor_id)
        pred_old_successor_id = old_successor_node.predecessor
        if pred_old_successor_id != None and self.in_between(pred_old_successor_id, self.id + 1, self.successor):
            self.successor = pred_old_successor_id
        old_successor_node.notify(self.id)
        self.transfer_keys()
        
        
    @method_logger
    def notify(self, node_id):
        """
        Verifies if node_id is a better predecessor, if it is then the predecessor is updated. 
        """
        if self.predecessor == None or self.in_between(node_id, self.predecessor + 1, self.id):
            self.predecessor = node_id
            predecessor_node = self.get_node_proxy(node_id)
            predecessor_node.transfer_keys()
    
    @method_logger
    def fix_fingers_loop(self):
        """
        Periodically calls fix_fingers method
        """
        interval_milliseconds = 2000
        while self.running:
            try:
                self.fix_fingers()
            except Exception as exc:
                log.exception(exc)
            time.sleep(interval_milliseconds/1000)
    
    @method_logger
    def fix_fingers(self):
        """
        Randomly updates a finger table's entry
        """
        if self.bits < 2:
            return
        update_index = random.randint(2, self.bits)
        finger_table_entry = self.finger_table[update_index]
        finger_table_entry.successor = self.find_successor(finger_table_entry.start)
    
    @method_logger
    def init_finger_table(self, initial_node):
        """
        Fill the node's finger_table using initial_node.
        """
        self.finger_table = [FingerTableEntry(self.id, i, self.bits, None) for i in range(self.bits+1)]
        
        self.successor = initial_node.find_successor(self.finger_table[1].start)
        successor_node = self.get_node_proxy(self.successor)
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
    def init_node_last_part(self):
        """
        Update others finger table nodes with current node.    
        Transfer the associated keys into this node.  
        """
        self.update_others()
        self.transfer_keys()
    
    @method_logger
    def update_others(self):
        """
        Update finger tables of nodes that should include this node
        """
        for i in range(1, self.bits + 1):
            # In the paper the +1 at the of 2**(i-1) doesn't exist but try example CHORD 3 then CHORD 5 and the FT of 3 doesn't update properly
            pred_id = self.find_predecessor((self.id - (1 << (i-1)) + 1) % (1 << self.bits))
            pred_node = self.get_node_proxy(pred_id)
            pred_node.update_finger_table(self.id, i)
            
    @method_logger
    def transfer_keys(self):
        """
        Brings the successor key values for what this node is responsible.
        """
        successor_id = self.find_successor(self.successor)
        successor_node = self.get_node_proxy(successor_id)
        new_keys = successor_node.pop_keys(self.predecessor + 1, self.id)
        self.values.update(new_keys)
    
    @method_logger
    def pop_keys(self, lower_bound:int, upper_bound:int):
        """
        Remove associated values within lower_bound and upper_bound. Both limits included
        
        Return the removed part of the dictionary.
        """
        keys = [x for x in self.values if self.in_between(x, lower_bound, upper_bound + 1)]
        values = [self.values[x] for x in keys]
        for k in keys:
            self.values.__delitem__(k)
        
        if keys:
            self.executor.submit(self.notify_listeners, keys)
        
        return {k:v for k,v in zip(keys, values)}
                
    @method_logger
    def update_finger_table(self, s:int, i:int):
        """
        Updates figer table at i if s is better suited
        """
        if self.id == s:
            return
        
        if self.in_between(s, self.id, self.finger_table[i].successor):
            self.finger_table[i].successor = s
            pred_node = self.get_node_proxy(self.predecessor)
            pred_node.update_finger_table(s, i)
                
    def get_node_proxy(self, id:int):
        """
        Returns a Chord Node proxy for the given id
        """
        if id != self.id:
            node = create_object_proxy(ChordNode.node_name(id))
        else:
            node = self
        return node
    
    def add_successor_list(self, successor_id):
        """
        Add successor_id to successor list
        """
        self.successor_list.append(successor_id)
        self.successor_list = list(set(self.successor_list))
        self.successor_list.sort(key=lambda x: x+(1 << self.bits) if x < self.id else x)
        self.successor_list = self.successor_list[:self.max_successor_list_count]
    
    def successor_node_from_succesor_list(self, key):
        """
        Returns the best posible node for key. It looks to the successor_list. 
        """
        nodes_ids = self.successor_list.copy()
        for node_id in nodes_ids:
            if self.in_between(key, self.id, node_id):
                try:
                    node = self.get_node_proxy(node_id)
                    return node
                except Exception as exc:
                    log.error(f"Node {node_id} offline.")
                    try:
                        self.successor_list.remove(node_id)
                    except ValueError:
                        pass
        raise ValueError(f"No available successor node for key {key}")
                    
                    