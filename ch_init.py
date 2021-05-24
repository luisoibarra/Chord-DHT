from ch_coord import ChordCoordinator
from ch_node import ChordNode
import logging as log

class DummyListener:
    
    def keys_relocated(self, keys):
        """
        Do whatever you want with the relocated keys
        """
        log.info(f"LISTENER Relocated {keys}")
    

if __name__ == "__main__":
    import sys
    log.basicConfig(level=log.DEBUG)
    try:
        if len(sys.argv) > 1:
            forced_id = int(sys.argv[1])
            ch1 = ChordNode(forced_id)
        else:
            ch1 = ChordNode()
        ch1.register_listener(DummyListener())
        ch1.start(ChordCoordinator.ADDRESS)
    except Exception as exc:
        log.exception(exc)
    