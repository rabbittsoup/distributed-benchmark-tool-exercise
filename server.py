"""server.py

The server script listens for client connections, logging performance data to server.log.  An arbitrary \
unused port is selected if not specified.

Usage:

python server.py [option]

Options and arguments:
-p         : server port to listen on
-c         : maximum concurrent client connections
--shutdown : attempt to shutdown a local server at the specified port
"""

from __future__ import print_function

import sys
import getopt
import os
import time
import socket
import threading
import SocketServer
import traceback

class RepeatingTimer(threading._Timer):
    def run(self):
        while True:
            self.finished.wait(self.interval)
            if (self.finished.is_set()): break
            self.function(*self.args, **self.kwargs)

class Server(SocketServer.ThreadingTCPServer):
    """Server(server_address, log = "server.log")
    
    Server is a SocketServer.ThreadingTCPServer.  Server implements the context
    manager interface and expects the __enter__() method to be called before
    starting the server with serve_forever().  The __exit__() method is also
    expected to be called after server_close() to release resources and cleanup.
    
    Aside from logging all client messages, Server will keep track of clients
    that send a START message, noting also the last time each client sent an
    ALIVE message.  After all tracked clients send a STOP message, or fail to
    send an ALIVE message within 10 seconds, Server will shut itself down.  The
    clients attribute contains lost clients after shut down.

    DATA messages are tracked, and each client's combined performance data is
    available in the data attribute.
    """

    def __init__(self, server_address, log = "server.log"):
        SocketServer.ThreadingTCPServer.__init__(self, server_address, Handler)
        self.log = log
        self.clients = {}
        self.data = {}
        self.lock = threading.Lock()
        self.datalock = threading.Lock()
        self.loglock = threading.Lock()
        self.heartbeattimer = RepeatingTimer(10.0, self.heartbeat)

    def logger(self, message):
        try:
            with self.loglock:
                message = ": ".join((time.strftime("%Y-%m-%d %H:%M:%S"), message))
                self.logfile.write(message)
                self.logfile.write("\n")
                
        except IOError:
            print(message)

    def heartbeat(self):
        with self.lock:
            t = time.time()
            # filter the clients that have not had an ALIVE message recently
            lost = {client for client, last in self.clients.iteritems() if ((t - last) > 10.0)}
            # if the filtered set is not all of the clients, don't shut down
            if (len(lost) < len(self.clients)): lost = set()

        # if all the clients are lost, we can shut down
        if (lost):
            self.shutdown()
            for client, pid in lost:
                self.logger(": ".join((client, pid, "LOST")))

    def __enter__(self):
        self.logfile = open(self.log, "a")
        self.heartbeattimer.start()
        return self

    def __exit__(self, type, value, traceback):
        self.server_close()
        self.heartbeattimer.cancel()
        self.heartbeattimer.join()
        self.logfile.close()
        return False

class Handler(SocketServer.StreamRequestHandler):
    def handle(self):
        server = self.server
        client = self.client_address[0]
        message = self.rfile.readline().strip()
        server.logger(": ".join((client, message)))
        tokens = [token.strip() for token in message.split(':')]
        client = (client, tokens[0])
        cmd = tokens[1] if (len(tokens) > 1) else None
        if ((cmd == "DATA") and (len(tokens) > 2)):
            # try to convert the payload to Python objects
            try:
                name, err, size, t, mbps = eval(tokens[2], {}, {})
                if (err is not None): raise ValueError(err)

            except:
                pass

            else:
                with server.datalock:
                    try:
                        data = server.data[client]
    
                    except KeyError:
                        pass

                    else:
                        # accumulate the data for this client
                        server.data[client] = (data[0] + 1, data[1], data[2] + size, data[3] + t)

        elif (cmd == "ALIVE"):
            with server.lock:
                if (client in server.clients):
                    server.clients[client] = time.time()

        elif ((cmd == "START") and (len(tokens) > 2)):
            # try to convert the payload to Python objects
            try:
                chunksize, maxsize = eval(tokens[2], {}, {})

            except:
                pass

            else:
                # start tracking this client
                with server.lock:
                    server.clients[client] = time.time()

                with server.datalock:
                    server.data[client] = (0, chunksize, 0, 0.0)

        elif (cmd == "STOP"):
            # stop tracking this client, shutdown if all clients are done
            with server.lock:
                server.clients.pop(client, None)
                shutdown = not server.clients

            if (shutdown): server.shutdown()

        elif (cmd == "SHUTDOWN"):
            server.shutdown()

def shutdown(server_address):
    """shutdown(server_address)
    
    Send a SHUTDOWN message to the given server.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(server_address)
        sock.sendall(": ".join((str(os.getpid()), "SHUTDOWN")))

    finally:
        sock.close()

    return 0

def main(argv):
    host = "localhost"
    port = 0
    stop = False

    try:
        opts, args = getopt.getopt(argv[1:], "hp:c:", ["help", "shutdown"])

        for o, a in opts:
            if (o == "-p"):
                port = int(a)
            
            elif (o == "-c"):
                Server.request_queue_size = int(a)
                
            elif (o == "--shutdown"):
                stop = True

            elif (o in ("-h", "--help")):
                print(main.__doc__)
                return 2

            else:
                raise getopt.GetoptError("option {} not recognized".format(o), o)

    except (getopt.GetoptError, ValueError, TypeError) as e:
        print(main.__doc__)
        for line in traceback.format_exception_only(type(e), e): print(line, end = '', file = sys.stderr)
        return 2

    if (stop):
        return shutdown((host, port))

    with Server((host, port)) as server:
        print("{} listening on port {}".format(*server.server_address))
        print("Up to {} concurrent client connections supported".format(server.request_queue_size))
        sys.stdout.flush()    # make sure this gets output in case, I don't know, a test or something needs it
        server.serve_forever()
        if (server.clients):
            print("{:<22}{:<17}".format(
                "Lost",
                "Last Seen",
            ))
            for (client, pid), last in server.clients.iteritems():
                print("{:<22}{:<17}".format(
                    ":".join((client, pid)),
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last)),
                ))

        print("{:<22}{:<6}{:<12}{:<14}{:<10}{:<10}".format(
            "Data",
            "Files",
            "Chunk",
            "Written",
            "Time",
            "MBps",
        ))
        if (server.data):
            for (client, pid), (files, chunksize, bytes, t) in server.data.iteritems():
                print("{:<22}{:<6}{:<12}{:<14}{:<10.3f}{:<10.3f}".format(
                    ":".join((client, pid)),
                    files,
                    chunksize,
                    bytes,
                    t,
                    0.0 if (t == 0.0) else ((bytes / t) / 1024**2),
                ))

        else:
            print("No data collected")

    return 0

main.__doc__ = __doc__

if (__name__ == "__main__"):
    sys.exit(main(sys.argv))
