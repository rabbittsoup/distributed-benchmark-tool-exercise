"""server_test.py

Runs server.py in a separate process and connects to it with the number of threads specified.  Each thread \
sends START, ALIVE, DATA, and STOP messages with known data at random times.  The stdout of the server \
process is then validated against the known data.

Usage:

python server_test.py [option]

Options and arguments:
-p     : port the server should listen on
-n     : number of threads to test with
"""

from __future__ import print_function

import sys
import getopt
import os
import time
import socket
import threading
import random
import subprocess
import Queue
import traceback

class ValidationError(Exception): pass

class Thread(threading.Thread):
    def __init__(self, server_address):
        super(Thread, self).__init__()
        self.server_address = server_address
        self.messages = Queue.Queue()

    def run(self):
        # consume messages from our queue until a None message tells us to stop
        while True:
            message, pause = self.messages.get()
            if (message is None): break
            message = ": ".join((self.name, message))
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect(self.server_address)
                # insert a pause after connect to have connections overlap
                if (pause): time.sleep(pause)
                try:
                    sock.sendall(message + '\n')

                finally:
                    sock.shutdown(socket.SHUT_RDWR)

            except socket.error as e:
                print(": ".join((self.name, "ERROR", str(e))))
                break

            finally:
                sock.close()
                sock = None

def main(argv):
    host = "localhost"
    port = 0
    n = 5

    try:
        opts, args = getopt.getopt(argv[1:], "hp:n:", ["help"])

        for o, a in opts:
            if (o == "-p"):
                port = int(a)
                
            elif (o == "-n"):
                n = int(a)
                
            elif (o in ("-h", "--help")):
                print(main.__doc__)
                return 2

            else:
                raise getopt.GetoptError("option {} not recognized".format(o), o)

        if (n <= 0): n = 5

    except getopt.GetoptError as e:
        print(main.__doc__)
        for line in traceback.format_exception_only(type(e), e): print(line, end = '', file = sys.stderr)
        return 2

    try:
        # run server
        serverp_args = [
            'python',
            'server.py',
            '-p', str(port),
            '-c', str(n),
        ]
        serverp = subprocess.Popen(serverp_args, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        info = [serverp.stdout.readline() for x in xrange(2)]
        # parse stdout to get port number
        if (port == 0):
            port = int(info[0].split()[4])

        # start our mini clients and push some test messages in their queues
        threads = [(x, Thread((host, port))) for x in xrange(n)]
        for i, thread in threads:
            thread.start()
            message = ": ".join(("START", repr((i, i + 1))))
            thread.messages.put((message, random.random() * 5))
            message = "ALIVE"
            thread.messages.put((message, random.random() * 5))
            message = ": ".join(("DATA", repr((thread.name, None, i * 1024**2, i + 1, float(i) / (i + 1)))))
            thread.messages.put((message, random.random() * 5))
            # send a DATA message with an error to verify it is not accumulated
            if (i == 1):
                message = ": ".join(("DATA", repr((thread.name, "ERR", i * 1024, i + 1, float(i) / (i + 1)))))
                thread.messages.put((message, random.random() * 5))

            message = ": ".join(("DATA", repr((thread.name, None, i * 1024**2, i + 2, float(i) / (i + 2)))))
            thread.messages.put((message, random.random() * 5))
            # don't send a STOP message to check that the server recognizes it as lost
            if (i != 1):
                message = "STOP"
                thread.messages.put((message, random.random() * 5))

            message = None
            thread.messages.put((message, 0))

        # wait for our mini clients and the server to complete
        for i, thread in threads: thread.join()
        stdout, stderr = serverp.communicate()
        print("stdout")
        if (stdout): print(stdout, end = '')
        print("stdout")
        print("stderr")
        if (stderr): print(stderr, end = '')
        print("stderr")
        # check return code
        print("return code is {}".format(serverp.returncode))
        if (serverp.returncode != 0): raise ValidationError("expected return code 0")
        if (not stdout): raise ValidationError("expected stdout")
        if (stderr): raise ValidationError("not expecting stderr")

        o = 0
        lines = [line.split() for line in stdout.splitlines()]
        if (not lines[-1]): del lines[-1]
        print("stdout has {} lines".format(len(lines)))
        # check for the lost mini client
        if (n > 1):
            if (len(lines) < 2): raise ValidationError("expected at least 2 lines")
            print("stdout line 0: {}".format(lines[0]))
            if (len(lines[0]) != 3): raise ValidationError("expected 3 tokens")
            if (lines[0][0] != "Lost"): raise ValidationError("expected Lost header line")
            print("stdout line 1: {}".format(lines[1]))
            if (len(lines[1]) != 3): raise ValidationError("expected 3 tokens")
            if (not lines[1][0].endswith(threads[1][1].name)): raise ValidationError("expected 1st token to end with {}".format(repr(threads[1][1].name)))
            del lines[:2]
            o += 2

        # check the accumulated data
        # the data can be reported in any order, so we keep track of each one as it's seen
        # then check at the end that we saw them all
        if (len(lines) != (n + 1)): raise ValidationError("expected {} Data lines".format(n + 1))
        print("stdout line {}: {}".format(o, lines[0]))
        if (len(lines[0]) != 6): raise ValidationError("expected 6 tokens")
        if (lines[0][0] != "Data"): raise ValidationError("expected Data header line")
        del lines[0]
        o += 1
        s = set()    # there should be only 1 occurance of each mini client
        for i, line in enumerate(lines):
            print("stdout line {}: {}".format(i + o, line))
            if (len(line) != 6): raise ValidationError("expected 6 tokens")
            try:
                j = int(line[2])

            except ValueError:
                raise ValidationError("expected 3rd token to be an integer")

            if (j in s): raise ValidationError("not expecting repeated 3rd token")
            s.add(j)
            if (not line[0].endswith(threads[j][1].name)): raise ValidationError("expected 1st token to end with {}".format(repr(threads[j][1].name)))
            if (line[1] != '2'): raise ValidationError("expected 2nd token to be 2")
            if (line[2] != str(j)): raise ValidationError("expected 3rd token to be {}".format(j))
            if (line[3] != str(j * 2 * 1024**2)): raise ValidationError("expected 4th token to be {}".format(j * 2 * 1024**2))
            if (line[4] != "{:.3f}".format((j * 2) + 3)): raise ValidationError("expected 5th token to be {:.3f}".format((j * 2) + 3))
            if (line[5] != "{:.3f}".format(float(j * 2) / ((j * 2) + 3))): raise ValidationError("expected 6th token to be {:.3f}".format(float(j * 2) / ((j * 2) + 3)))

        # check that we saw all the mini clients, no more, no less
        if (sorted(s) != range(n)): raise ValidationError("expected range({})".format(n))

    except ValidationError as e:
        for line in traceback.format_exception_only(type(e), e): print(line, end = '', file = sys.stderr)
        print("FAIL")
        return 1

    else:
        print("PASS")
        return 0

main.__doc__ = __doc__

if (__name__ == "__main__"):
    sys.exit(main(sys.argv))
