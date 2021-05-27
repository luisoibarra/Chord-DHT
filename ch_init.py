from ch_coord import ChordCoordinator
from ch_node import ChordNode
import logging as log
import plac

class DummyListener:
    
    def keys_relocated(self, keys):
        """
        Do whatever you want with the relocated keys
        """
        log.info(f"LISTENER Relocated {keys}")
    

# plac annotation (description, type of arg [option, flag, positional], abrev, type, choices)
def main(host:("Chord node host","option","ho",str)=None,
         port:("Chord node port","option","p",str)=0,
         ns_host:("Name server host","option","nsh",str)=None,
         ns_port:("Name server port","option","nsp",str)=None,
         forced_id:("Force the node id","option","id",int)=None):
    log.basicConfig(level=log.DEBUG,format='[%(asctime)s] %(levelname)s - %(message)s')
    try:
        ch1 = ChordNode(host, port, ns_host, ns_port, forced_id)
        ch1.register_listener(DummyListener())
        ch1.start(ChordCoordinator.ADDRESS)
    except Exception as exc:
        log.exception(exc)
    

if __name__ == "__main__":
    plac.call(main)
