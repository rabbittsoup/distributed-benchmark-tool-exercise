"""client.py

The client script continuously measures write performance, submitting the results to a server, as well as \
logging to client_{pid}.log.  The client will run the specified run time, or defaults to a random time \
that will write at least 2 files if not provided.  Writing is performed in chunks to a temporary file.  \
The chunk size can be specified, or will default to a random size between 10MB and 20MB.  The file will be \
closed when the number of chunks written exceeds the maximum file size, and a new temporary file will be \
opened, repeating the process.  The maximum file size will default to a random size between the chunk size \
and 100 times the chunk size if not specified.

Usage:

python client.py [option] host port

Options and arguments:
host   : server host address
port   : server port
-t     : run time, in seconds
-c     : size of each chunk to write, in MB
-m     : maximum file size, approximately, in MB
-d     : delete the files as they are closed
"""

from __future__ import print_function

import sys
import getopt
import os.path
import time
import socket
import threading
import tempfile
import random
import traceback
import subprocess
import multiprocessing

class EventThread(threading.Thread):
    def __init__(self, target = None, name = None, args = (), kwargs = {}):
        super(EventThread, self).__init__(target = target, name = name, args = args, kwargs = kwargs)
        self.finished = threading.Event()

    def cancel(self):
        self.finished.set()

    def run(self):
        super(EventThread, self).run()
        self.finished.set()

class RepeatingTimer(EventThread):
    def __init__(self, interval, target, args = (), kwargs = {}):
        super(RepeatingTimer, self).__init__()
        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.interval = interval

    def run(self):
        try:
            while True:
                self.finished.wait(self.interval)
                if (self.finished.is_set()): break
                self.target(*self.args, **self.kwargs)

        finally:
            del self.target, self.args, self.kwargs

class Client(object):
    """Client(server_address, chunk, maxsize, minfiles = 0, delete = True, log = "client_{pid}.log")
    
    Client writes to a temporary file in chunk increments.  After the number of
    chunks written exceeds the maxsize, the file is closed and the performance
    data for the file write is logged and a DATA message sent to a server.  A
    new temporary file is created and the process repeats.

    Client sends a START message before beginning the writing process, and sends
    an ALIVE message every 5 seconds.  STATUS messages with process information
    about the data collection process are sent every 10 seconds.

    Client implements the context manager interface and expects the __enter__()
    method to be called to start the process.  The __exit__() method is expected
    to be called to shut down and clean up Client.  Shut down will block until
    minfiles have been successfully written or too many write errors in a row
    occur.
    
    The chunk data is expected to be a multiprocessing.Array().
    """

    def __init__(self, server_address, chunk, maxsize, minfiles = 0, delete = True, log = None):
        self.server_address = server_address
        self.chunksize = len(chunk)
        self.maxsize = maxsize
        self.log = "client_{}.log".format(os.getpid()) if (log is None) else log
        self.finish = multiprocessing.Event()
        self.loggerq = multiprocessing.Queue()    # items must be (cmds, message), cmds = '[SWQ]+'
        self.loggerthread = threading.Thread(target = self.logger)
        self.heartbeattimer = RepeatingTimer(5.0, self.heartbeat)
        self.statustimer = RepeatingTimer(10.0, self.status)
        self.workerthread = multiprocessing.Process(target = self.worker, args = (chunk, maxsize, minfiles, delete, self.finish, self.loggerq))

    def write(self, file, message):
        message = ": ".join((time.strftime("%Y-%m-%d %H:%M:%S"), message))
        try:
            file.write(message)
            file.write("\n")

        except IOError:
            print(message)

    def sendall(self, file, message):
        message = ": ".join((str(os.getpid()), message))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(self.server_address)
            try:
                sock.sendall(message + '\n')

            finally:
                sock.shutdown(socket.SHUT_RDWR)

        except socket.error as e:
            self.write(file, ": ".join(("ERROR", str(e))))

        finally:
            sock.close()
            sock = None

    def logger(self):
        # logger runs in a thread, consuming messages from the loggerq
        # S to send to server, W to write to log, Q to quit the thread
        with open(self.log, "a") as file:
            cmd = None
            while (cmd is not 'Q'):
                cmds, message = self.loggerq.get()
                for cmd in cmds:
                    if (cmd is 'S'):
                        self.sendall(file, message)

                    elif (cmd is 'W'):
                        self.write(file, message)

    def heartbeat(self):
        message = "ALIVE"
        self.loggerq.put(('S', message))

    # sys.platform may not be granular enough depending on environment expectations
    if (sys.platform == "linux2"):
        def status(self):
            message = "STATUS"
            if (self.workerthread.is_alive()):
                # run top to get process information on the worker 'thread'
                # it seems this information may be available via procfs instead, but %CPU seems fidly
                stdout, stderr = subprocess.Popen(['top', '-p', str(self.workerthread.pid), '-n', '2', '-d', '0.001', '-b'], stdout = subprocess.PIPE, stderr = subprocess.PIPE).communicate()
                lines = stdout.splitlines()
                if (not lines[-1]): del lines[-1]
                if (lines and lines[0].startswith('top -') and (len(lines) >= 2)):
                    keys = lines[-2].split()
                    if (keys and (keys[-1] == "COMMAND")):
                        values = lines[-1].split(None, len(keys) - 1)    # split up to command
                        data = {k: v for k, v in zip(keys, values)}
                        message = ": ".join((message, repr(data)))

            self.loggerq.put(('S', message))

    # macOS
    elif (sys.platform == "darwin"):
        def status(self):
            message = "STATUS"
            if (self.workerthread.is_alive()):
                stdout, stderr = subprocess.Popen(['top', '-pid', str(self.workerthread.pid), '-l', '2', '-s', '0'], stdout = subprocess.PIPE, stderr = subprocess.PIPE).communicate()
                lines = stdout.splitlines()
                if (not lines[-1]): del lines[-1]
                if (lines and lines[0].startswith('Processes:') and (len(lines) >= 2)):
                    keys = lines[-2].split()
                    if (keys and (keys[1] == "COMMAND")):
                        values = lines[-1].split(None, 1)    # split up to command
                        values = [values[0]] + values[1].rsplit(None, len(keys) - 2)  # right split back to command
                        data = {k: v for k, v in zip(keys, values)}
                        message = ": ".join((message, repr(data)))

            self.loggerq.put(('S', message))

    else:
        def status(self):
            message = "STATUS"
            self.loggerq.put(('S', message))

    @staticmethod
    def worker(chunk, maxsize, minfiles, delete, finish, loggerq):
        """worker(chunk, maxsize, minfiles, delete, finish, loggerq)
        
        Continuously writes chunks of data to temporary files, measuring the
        time to perform os.write()/os.fsync() calls.  The combined time for all
        chunk writes to a single file is logged and sent to a server as a DATA
        message.
        
        Arguments:
        chunk    : A multiprocessing.Array() of the chunk data.
        maxsize  : Integer.  Rollover file when at least maxsize bytes are written.
        minfiles : Integer.  Keep working until at least minfiles are written.
        delete   : Boolean.  Indicates whether to delete files when closed.
        finish   : A multiprocessing.Event() signaling the worker to end.
        loggerq  : A multiprocessing.Queue() to place log messages.
        """
        global len
        lap = time.time
        write = os.write    # use os.write()/os.fsync() to limit the layers getting involved
        fsync = os.fsync
        finish = finish
        chunk = chunk
        maxsize = maxsize
        minfiles = minfiles
        files = 0
        errored = 0
        # stop writing when finish event is set
        # and we reached our minfiles or errored too many times in a row
        while (((files < minfiles) and (errored < minfiles)) or (not finish.is_set())):
            with chunk.get_lock():
                data = chunk.get_obj()
                datasize = len(data)
                # keep track of time markers around os.write()/os.fsync()
                laps = [None] * ((maxsize + (datasize - 1)) // datasize)
                err = None
                size = 0
                i = 0
                with tempfile.NamedTemporaryFile(delete = delete) as tmp:
                    fileno = tmp.fileno()
                    try:
                        while ((size < maxsize) and ((files < minfiles) or (not finish.is_set()))):
                            # if os.write() returns a size lower than expected,
                            # then our list length may need to be extended
                            if (i >= len(laps)): laps.append(None)
                            start = lap()
                            size += write(fileno, data)
                            fsync(fileno)
                            end = lap()
                            laps[i] = (start, end)
                            i += 1

                    except OSError as e:
                        err = str(e)

            # if we errored out, remove empty slots
            del laps[i:]
            # calculate total time over all writes to this file
            t = 0.0
            for start, end in laps: t += end - start
            # calculate MBps
            mbps = 0.0 if (t == 0.0) else ((size / t) / 1024**2)
            message = ": ".join(("DATA", repr((tmp.name, err, size, t, mbps))))
            loggerq.put(('SW', message))
            # increment successful files, reset errored on success
            if (err is None):
                files += 1
                errored = 0

            else:
                errored += 1

    def __enter__(self):
        self.loggerthread.start()
        message = ": ".join(("START", repr((self.chunksize, self.maxsize))))
        self.loggerq.put(('SW', message))
        self.heartbeattimer.start()
        self.statustimer.start()
        self.workerthread.start()
        return self

    def __exit__(self, type, value, traceback):
        self.finish.set()
        # wait for the worker to be done since it may take a bit
        self.workerthread.join()
        self.statustimer.cancel()
        self.heartbeattimer.cancel()
        self.statustimer.join()
        self.heartbeattimer.join()
        message = "STOP"
        self.loggerq.put(('SWQ', message))    # 'Q' tells loggerthread to stop
        self.loggerthread.join()
        return False

def main(argv):
    runtime = None
    chunksize = random.randrange(10 * 1024**2, 20 * 1024**2)
    maxsize = random.randrange(chunksize, chunksize * 100)
    delete = False

    try:
        opts, args = getopt.getopt(argv[1:], "ht:c:m:d", ["help"])

        for o, a in opts:
            if (o == "-t"):
                runtime = float(a)
                
            elif (o == "-c"):
                chunksize = int(float(a) * 1024**2)
            
            elif (o == "-m"):
                maxsize = int(float(a) * 1024**2)

            elif (o == "-d"):
                delete = True

            elif (o in ("-h", "--help")):
                print(main.__doc__)
                return 2

            else:
                raise getopt.GetoptError("option {} not recognized".format(o), o)

        if (len(args) < 2):
            raise getopt.GetoptError("expected host and port", "")

        host = args[0]
        port = int(args[1])

        chunk = multiprocessing.Array('B', chunksize)

        # when individual write performance has a large distribution
        # or if the environment changes during testing
        # then mintime may not gaurantee 2 files will be written
        mintime = t = i = 0
        with tempfile.NamedTemporaryFile(delete = True) as tmp:
            fileno = tmp.fileno()
            # if we're really fast, write for a bit to make sure
            while ((mintime < 1) and (t < 2)):
                start = time.time()
                os.write(fileno, chunk.get_obj())
                os.fsync(fileno)
                end = time.time()
                i += 1
                t += end - start
                # estimate mintime to write 2 files
                mintime = (t / i) * ((maxsize + (chunksize - 1)) // chunksize) * 2 * 1.25

        if (runtime is None):
            mintime = int(mintime + 0.5) if (mintime > 1) else 1
            runtime = random.randrange(mintime, mintime * 6)

        elif ((runtime <= 0) or (runtime < mintime)):
            raise ValueError("expected {:.0f}s minimum run time for {} file size".format(mintime, maxsize))

    except (getopt.GetoptError, ValueError, TypeError) as e:
        print(main.__doc__)
        for line in traceback.format_exception_only(type(e), e): print(line, end = '', file = sys.stderr)
        return 2

    # run client and block until at least 2 files are written
    with Client((host, port), chunk, maxsize, 2, delete) as client:
        time.sleep(runtime)

    return 0

main.__doc__ = __doc__

if (__name__ == "__main__"):
    sys.exit(main(sys.argv))
