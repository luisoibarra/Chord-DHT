import Pyro4 as pyro
from ch_coord import ChordCoordinator
from ch_node import ChordNode
from ch_shared import create_object_proxy
import plac

NS_HOST=None
NS_PORT=None

def get_chord_node():
    coordinator = create_object_proxy(ChordCoordinator.ADDRESS, NS_HOST, NS_PORT)
    id = coordinator.get_initial_node()
    if id == None:
        print("Chord DHT is empty")
        return
    node = create_object_proxy(ChordNode.node_name(id), NS_HOST, NS_PORT)
    return node

def get_value(key):
    node = get_chord_node()
    value = node.lookup(key)
    return value

def save_value(value, key):
    node = get_chord_node()
    node.insert(value, key)
    
def main(ns_host:("Pyro name server host","option","nsh",str)=None,
         ns_port:("Pyro name server port","option","nsp",int)=None):
    NS_HOST = ns_host
    NS_PORT = ns_port
    import sys
    command = None
    while True:
        help_msg = "commands:\n" + "\n".join(["- " + x for x in ["save", "get", "key", "exit"]])
        command = input(">> ")
        if command == "exit":
            break
        command_words = command.split()
        if len(command_words) > 0:
            node = get_chord_node()
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
                save_value(value, key)
            elif command_words[0] == "get":
                key = None
                if len(command_words) > 1:
                    key = command_words[1]
                else:
                    print("Missing args: get value  Get the value from the DHT, Warning when getting a value with a forced key")
                    continue
                value = get_value(key)
                print(value)
            elif command_words[0] == "key":
                key = None
                if len(command_words) > 1:
                    key = command_words[1]
                else:
                    print("Missing args: key key  Get the value for the integer key")
                    continue
                value = get_value(int(key))
                print(value)
        else:
            print(help_msg)
                

if __name__ == "__main__":
    plac.call(main)
                
                
                