import subprocess
import os
import signal
import time

terminal = "gnome-terminal"
python = "python3"

bits = 4
ids = [0, 1, 5, 10, 14]

init_node_args = ["ch_init.py", "-id", ]
init_coord_args = ["ch_coord.py", "-b", str(bits)]
init_ns_args = ["ch_ns.py"]

terminal_args = [terminal, "--", python, ]

pids = []

pid = os.fork()
if not pid:
    # Start name server
    print("Creating name server")
    subprocess.call(terminal_args + init_ns_args)
    exit()
else:
    pids.append(pid)
    time.sleep(1)
    pid = os.fork()
    if not pid:
        # Start coordinator
        print("Creating coordinator")
        subprocess.call(terminal_args + init_coord_args)
        exit()
    else:
        pids.append(pid)
        for id in ids:
            print("Creating node",id)
            time.sleep(.5)
            pid = os.fork()
            if not pid:
                subprocess.call(terminal_args + init_node_args + [str(id)])
                exit()
            pids.append(pid)

print("pids:",pids)