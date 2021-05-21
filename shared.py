import zmq
import pickle

class Message:
    
    LOOKUP_REQ = '1'
    LOOKUP_REP = '2'
    JOIN       = '3'
    LEAVE      = '4'
    ANNOUNCE   = '5'
    STOP       = '6'
    
    def __init__(self, action:str):
        self.action = action


def reliable_send(message:Message, direction:str):
    pass