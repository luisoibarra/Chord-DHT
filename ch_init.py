from ch_coord import ChordCoordinator
from ch_node import ChordNode


if __name__ == "__main__":
    import sys
    import logging as log
    log.basicConfig(level=log.DEBUG)
    try:
        if len(sys.argv) > 1:
            forced_id = int(sys.argv[1])
            ch1 = ChordNode(forced_id)
        else:
            ch1 = ChordNode()
        ch1.start(ChordCoordinator.ADDRESS)
    except Exception as exc:
        log.exception(exc)
    