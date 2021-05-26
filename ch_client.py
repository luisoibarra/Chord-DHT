import Pyro4 as pyro
from ch_coord import ChordCoordinator
from ch_node import ChordNode
from ch_shared import create_object_proxy
import plac

class ChordSimpleConsumer:
    def __init__(self, ns_port, ns_host):
        self.ns_port = ns_port
        self.ns_host = ns_host
        self.coordinator = create_object_proxy(ChordCoordinator.ADDRESS, self.ns_host, self.ns_port)
        
        
    def get_chord_node(self):
        id = self.coordinator.get_initial_node()
        if id == None:
            print("Chord DHT is empty")
            return
        node = create_object_proxy(ChordNode.node_name(id), self.ns_host, self.ns_port)
        return node

    def get_value(self, key):
        node = self.get_chord_node()
        try:
            value = node.lookup(key)
            return value
        except Exception as exc:
            print(exc)
    
    def save_value(self, value, key):
        node = self.get_chord_node()
        try:
            node.insert(value, key)
        except Exception as exc:
            print(exc)

def main(ns_host:("Pyro name server host","option","nsh",str)=None,
         ns_port:("Pyro name server port","option","nsp",int)=None):
    client = ChordSimpleConsumer(ns_port, ns_host)
    import sys
    help_msg = "commands:\n" + "\n".join(["- " + x for x in ["save", "get", "key", "exit"]])
    command = None
    print(help_msg)
    while True:
        command = input(">> ")
        if command == "exit":
            break
        command_words = command.split()
        if len(command_words) > 0:
            if command_words[0] == "save":
                value = None
                key = None
                if len(command_words) > 1:
                    value = command_words[1]
                else:
                    print("Missing args: save value [key]  Saves the value in the DHT with optional forced key")
                    continue
                if len(command_words) > 2:
                    key = int(command_words[2])
                client.save_value(value, key)
            elif command_words[0] == "get":
                key = None
                if len(command_words) > 1:
                    key = command_words[1]
                else:
                    print("Missing args: get value  Get the value from the DHT, Warning when getting a value with a forced key")
                    continue
                value = client.get_value(key)
                print(value)
            elif command_words[0] == "key":
                key = None
                if len(command_words) > 1:
                    key = command_words[1]
                else:
                    print("Missing args: key key  Get the value for the integer key")
                    continue
                value = client.get_value(int(key))
                print(value)
        else:
            print(help_msg)
                

if __name__ == "__main__":
    plac.call(main)
                
                
                