import plac
import os

def main(ns_host:("Pyro name server host","option","nsh",str)=None,
         ns_port:("Pyro name server port","option","nsp",int)=None):
    args = ["python3", "-m", "Pyro4.naming"]
    if ns_host != None and ns_port != None:
        args.extend(["-n", ns_host, "-p", str(ns_port)])
    os.execvp("python3",args)
    
if __name__ == "__main__":
    plac.call(main)