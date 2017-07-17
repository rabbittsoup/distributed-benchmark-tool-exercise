"""client_test.py

Runs a simple server to collect messages from client.py, which is started in a separate process with a known \
random run time, chunk size, and maximum file size.  The messages received are then validated against the \
known data.  A START message is expected first, and a STOP message is expected last.  ALIVE and STATUS \
messages are tracked and must occur every 5s or 10s, respectively.  File sizes and message data is validated \
for DATA messages.

Usage:

python client_test.py [option]

Options and arguments:
-p     : port the client should use
"""

from __future__ import print_function

import sys
import getopt
import os.path
import time
import socket
import threading
import SocketServer
import random
import subprocess
import Queue
import traceback

class ValidationError(Exception): pass

class Server(SocketServer.ThreadingTCPServer):
    def __init__(self, server_address):
        SocketServer.ThreadingTCPServer.__init__(self, server_address, Handler)
        self.messages = Queue.Queue()
        self.servethread = threading.Thread(target = self.serve_forever)

    def __enter__(self):
        self.servethread.start()
        return self

    def __exit__(self, type, value, traceback):
        # give servethread another chance by sleeping a tiny bit
        # unnecessary on macos, but it seems ubuntu would exit the client process
        # and shutdown the server before the last message was received by the server
        # not a problem in the real server since it doesn't issue a shutdown until
        # after the last message is actually processed
        time.sleep(0.001)
        self.shutdown()
        self.servethread.join()
        self.server_close()
        return False

class Handler(SocketServer.StreamRequestHandler):
    def handle(self):
        # time stamp each message and save it in the queue
        message = self.rfile.readline().strip()
        self.server.messages.put((time.time(), message))

def main(argv):
    host = "localhost"
    port = 0
    chunksize = random.randrange(10 * 1024**2, 20 * 1024**2)
    maxsize = random.randrange(chunksize, chunksize * 100)
    delete = True
    
    try:
        opts, args = getopt.getopt(argv[1:], "hp:", ["help"])

        for o, a in opts:
            if (o == "-p"):
                port = int(a)
                
            elif (o in ("-h", "--help")):
                print(main.__doc__)
                return 2

            else:
                raise getopt.GetoptError("option {} not recognized".format(o), o)

    except getopt.GetoptError as e:
        print(main.__doc__)
        for line in traceback.format_exception_only(type(e), e): print(line, end = '', file = sys.stderr)
        return 2

    files = set()
    try:
        # convert to MB text for args
        chunksize = str(float(chunksize) / 1024**2)
        maxsize = str(float(maxsize) / 1024**2)

        with Server((host, port)) as server:
            port = server.server_address[1]

            # inconsistent write times can cause different mintimes between client runs
            while True:
                # run client with -t 0 option to get min run time
                clientp_args = [
                    'python',
                    'client.py',
                    '-t', '0',
                    '-c', chunksize,
                    '-m', maxsize,
                    '-d',
                    host,
                    str(port),
                ]
                clientp = subprocess.Popen(clientp_args, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
                stdout, stderr = clientp.communicate()
                if (clientp.returncode != 2):
                    print("stdout")
                    if (stdout): print(stdout, end = '')
                    print("stdout")
                    print("stderr")
                    if (stderr): print(stderr, end = '')
                    print("stderr")
                    print("return code is {}".format(clientp.returncode))
                    raise ValidationError("expected return code 2")

                if (not stderr):
                    print("stdout")
                    if (stdout): print(stdout, end = '')
                    print("stdout")
                    print("stderr")
                    if (stderr): print(stderr, end = '')
                    print("stderr")
                    raise ValidationError("expected stderr")

                # parse stderr to get min run time
                lines = [line.split() for line in stderr.splitlines()]
                if (not lines[-1]): del lines[-1]
                try:
                    if (len(lines[-1]) != 10): raise ValidationError("expected 10 tokens")
                    if (lines[-1][0] != "ValueError:"): raise ValidationError("expected 1st token to be 'ValueError:'")
                    try:
                        mintime = lines[-1][2]
                        if (mintime[-1] != 's'): raise ValueError()
                        mintime = int(mintime[:-1])
                        if (mintime == 0): mintime = 1
                        elif (mintime < 0): raise ValueError()

                    except ValueError:
                        raise ValidationError("expected 3rd token to be minimum run time")

                except ValidationError:
                    print("stdout")
                    if (stdout): print(stdout, end = '')
                    print("stdout")
                    print("stderr")
                    if (stderr): print(stderr, end = '')
                    print("stderr")
                    print("stderr line {}: {}".format(len(lines) - 1, lines[-1]))
                    raise

                # run client
                runtime = random.randrange(mintime * 2, mintime * 8)
                clientp_args = [
                    'python',
                    'client.py',
                    '-t', str(runtime),
                    '-c', chunksize,
                    '-m', maxsize,
                    host,
                    str(port),
                ]
                start = time.time()
                clientp = subprocess.Popen(clientp_args, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
                stdout, stderr = clientp.communicate()
                end = time.time()
                # if we get a mintime error on this run, try again
                if (clientp.returncode == 2):
                    lines = [line.split() for line in stderr.splitlines()]
                    if (not lines[-1]): del lines[-1]
                    if ((len(lines[-1]) == 10) and (lines[-1][0] == "ValueError:") and (lines[-1][2][-1] == 's')):
                        print("mintime inconsistency, retrying")
                        continue

                # move on
                break

            print("stdout")
            if (stdout): print(stdout, end = '')
            print("stdout")
            print("stderr")
            if (stderr): print(stderr, end = '')
            print("stderr")
            
            # check return code
            print("return code is {}".format(clientp.returncode))
            if (clientp.returncode != 0): raise ValidationError("expected return code 0")
            if (stdout): raise ValidationError("not expecting stdout")
            if (stderr): raise ValidationError("not expecting stderr")
            print("run time was {:.3f}s".format(end - start))
            # check run time, upper bound not always known since client makes sure 2 files are written
            if (runtime >= (end - start)):
                raise ValidationError("expected run time to be at least {}s".format(runtime))

        runtime = end - start
        # convert from MB text for calculations
        chunksize = int(float(chunksize) * 1024**2)
        maxsize = int(float(maxsize) * 1024**2)
        messages = server.messages

        print("received {} messages".format(messages.qsize()))
        # START, STOP, >= 2 DATA
        x = 4
        # ALIVE
        if (runtime >= 6): x += 1
        # STATUS
        if (runtime >= 11): x += 1
        if (messages.qsize() < x):
            raise ValidationError("expected at least {} messages".format(x))

        # START should be first message
        t, message = messages.get()
        message = [message.strip() for message in message.split(":", 2)]
        print("message 1: {}".format(message))
        if (len(message) != 3): raise ValidationError("expected 3 tokens")
        if (message[0] != str(clientp.pid)): raise ValidationError("expected 1st token to be {}".format(clientp.pid))
        if (message[1] != "START"): raise ValidationError("expected 2nd token to be START")
        if (message[2] != repr((chunksize, maxsize))):
            raise ValidationError("expected 3rd token to be {}".format(repr((chunksize, maxsize))))

        # loop through each message except for the last
        # keep track of the last alive and status times, and how many have been received
        alive = [t, 0]
        status = [t, 0]
        # keep track of file count, total size, total time, accumulated MBps
        data = [0, 0, 0, 0]
        for i in xrange(2, messages.qsize() + 1):
            t, message = messages.get()
            message = [message.strip() for message in message.split(":", 2)]
            print("message {}: {}".format(i, message))
            if (len(message) < 2): raise ValidationError("expected at least 2 tokens")
            if (message[0] != str(clientp.pid)): raise ValidationError("expected 1st token to be {}".format(clientp.pid))
            if (message[1] == "ALIVE"):
                if (not (3 <= (t - alive[0]) <= 7)):
                    print("message {}: received {:.3f}s after last ALIVE".format(i, t - alive[0]))
                    raise ValidationError("expected ALIVE {}".format("sooner" if ((t - alive[0]) > 5) else "later"))

                alive[0] = t
                alive[1] += 1

            elif (message[1] == "STATUS"):
                if (not (8 <= (t - status[0]) <= 12)):
                    print("message {}: received {:.3f}s after last STATUS".format(i, t - status[0]))
                    raise ValidationError("expected STATUS {}".format("sooner" if ((t - status[0]) > 10) else "later"))

                status[0] = t
                status[1] += 1

            elif (message[1] == "DATA"):
                if (len(message) != 3): raise ValidationError("expected 3 tokens")
                # try to convert the payload to Python objects
                try:
                    name, err, size, t, mbps = eval(message[2], {}, {})

                except:
                    raise ValidationError("expected 3rd token to be a repr(5-tuple)")

                else:
                    # check object types
                    if (not isinstance(name, str)): raise ValidationError("expected 1st item in 3rd token to be a str")
                    if (not isinstance(size, int)): raise ValidationError("expected 3rd item in 3rd token to be an int")
                    if (not isinstance(t, float)): raise ValidationError("expected 4th item in 3rd token to be a float")
                    if (not isinstance(mbps, float)): raise ValidationError("expected 5th item in 3rd token to be a float")
                    if (name in files): raise ValidationError("not expecting repeated 1st item of 3rd token")
                    files.add(name)
                    if (err is not None):
                        print("message {}: received error {}".format(i, err))

                    else:
                        try:
                            stat = os.stat(name)

                        except OSError:
                            raise ValidationError("expected 1st item in 3rd token to be an existing file")

                        print("message {}: file size {}".format(i, stat.st_size))
                        if (stat.st_size != size): raise ValidationError("expected file size to be {}".format(size))
                        if (size >= (maxsize + chunksize)): raise ValidationError("expected file size to be less than {}".format(maxsize + chunksize))
                        if (t <= 0): raise ValidationError("expected 4th item in 3rd token to be greater than 0")
                        if ("{:.3f}".format(size / t / 1024**2) != "{:.3f}".format(mbps)):
                            raise ValidationError("expected 5th item in 3rd token to equal 3rd item / 4th item / 1048576")

                        # attempt to verify mbps using file size / (file modified time - file creation time)
                        # this doesn't appear possible on posix since creation time is not available
                        #statmbps = stat.st_size / (stat.st_mtime - stat.st_ctime) / 1024**2
                        #print("message {}: calculated {:.3f} MBps from file stat".format(i, statmbps))
                        #if (not ((statmbps * 0.90) < mbps < (statmbps * 1.10))):
                        #    raise ValidationError("expected {:.3f} MBps".format(statmbps))

                        data[0] += 1
                        data[1] += size
                        data[2] += t
                        data[3] += mbps

            else:
                raise ValidationError("not expecting this message")

        # STOP should be last message
        t, message = messages.get()
        message = [message.strip() for message in message.split(":", 2)]
        print("message {}: {}".format(i + 1, message))
        if (len(message) != 2): raise ValidationError("expected 2 tokens")
        if (message[0] != str(clientp.pid)): raise ValidationError("expected 1st token to be {}".format(clientp.pid))
        if (message[1] != "STOP"): raise ValidationError("expected 2nd token to be STOP")

        # make sure ALIVE continued until DONE
        print("received {} ALIVE messages".format(alive[1]))
        if ((t - alive[0]) > 7):
            print("message {}: received {:.3f}s after last ALIVE".format(i + 1, t - alive[0]))
            raise ValidationError("expected more ALIVE messages")

        # make sure STATUS continued until DONE
        print("received {} STATUS messages".format(status[1]))
        if ((t - status[0]) > 12):
            print("message {}: received {:.3f}s after last STATUS".format(i + 1, t - status[0]))
            raise ValidationError("expected more STATUS messages")

        # check DATA
        print("received {} DATA messages".format(data[0]))
        if (data[0] < 2): raise ValidationError("expected at least 2 DATA messages")
        print("total write time {:.3f}s".format(data[2]))
        # check write time, adjust lower bound by fixed 2s to account for client test write at startup
        if (not (((runtime * 0.80) - 2) < data[2] <= runtime)):
            raise ValidationError("expected write time to be near {:.0f}s".format(runtime))

        # this check may be unreasonable when individual file performance has a large distribution
        # a warning seems more suitable unless sufficient statistics can be taken into account
        avgmbps = data[1] / data[2] / 1024**2
        print("averaged {:.3f} MBps".format(avgmbps))
        if (not ((avgmbps * 0.80) < (data[3] / data[0]) < (avgmbps * 1.20))):
            print("ValidationWarning: individual performance calculations averaged {:.3f} MBps".format(data[3] / data[0]), file = sys.stderr)

    except ValidationError as e:
        for line in traceback.format_exception_only(type(e), e): print(line, end = '', file = sys.stderr)
        print("FAIL")
        return 1

    else:
        print("PASS")
        return 0

    finally:
        if (delete):
            for name in files:
                try:
                    os.remove(name)

                except OSError:
                    pass

main.__doc__ = __doc__

if (__name__ == "__main__"):
    sys.exit(main(sys.argv))
