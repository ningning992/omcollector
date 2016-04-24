#!/usr/bin/env python
#
# omcollector.py
#
"""Simple manager for collection scripts that run and gather data. The
   omcollector gathers data and sends it to OpsMonitor for storage."""


import atexit
import errno
import fcntl
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import json
import struct
from logging.handlers import RotatingFileHandler
from Queue import Queue
from Queue import Empty
from Queue import Full
from optparse import OptionParser


# global variables.
COLLECTORS = {}
GENERATION = 0
DEFAULT_LOG = '/tmp/omcollector.log'
LOG = logging.getLogger('omcollector')
ALIVE = True
# If the SenderThread catches more than this many consecutive uncaught
# exceptions, omcollector will shutdown.
MAX_UNCAUGHT_EXCEPTIONS = 100
DEFAULT_PORT = 10051
# Seconds to wait for data before assuming a collector is dead and restart it
ALLOWED_INACTIVITY_TIME = 600
MAX_SENDQ_SIZE = 10000
MAX_READQ_SIZE = 100000


def register_collector(collector):
    """Register a collector with the COLLECTORS global"""

    assert isinstance(collector, Collector), 'collector=%r' % (collector,)
    # store it in the global dict and initiate a kill for anybody with the
    # same name that happens to still be running.
    # if not None, col.proc is the instance of subprocess.Popen
    if collector.name in COLLECTORS:
        col = COLLECTORS[collector.name]
        if col.proc is not None:
            LOG.error('%s still has a process (pid=%d) and is being reset,'
                      ' terminating', col.name, col.proc.pid)
            col.shutdown()

    LOG.info('Register collector: %s', collector.name)
    COLLECTORS[collector.name] = collector


class ReaderQueue(Queue):
    """A Queue for the reader thread"""

    def nput(self, value):
        """A nonblocking put, that simply logs and discards the value when
           the queue is full, and returns false if we dropped."""
        try:
            self.put(value, block=False)
            LOG.debug('Put into ReaderQueue: %s', value)
        except Full:
            LOG.error('QUEUE FULL ERROR, DROPPED DATA: %s', value)
            return False
        return True


class Collector(object):
    """A Collector is a script that gathers some data and prints it out in
       specified format on STDOUT.  This class maintains all of the state
       information for a given collector and provides some useful methods
       for working with it.
	   
       STDOUT FORMAT:
        :if no space in each field: 
         <hostname> <key> <value> <timestamp>
        :or with space in some field:
         <hostname>|<key>|<value>|<timestamp>
    """

    def __init__(self, colname, interval, filename, mtime=0, lastspawn=0):
        """Construct a new Collector."""

        self.name = colname
        self.interval = interval
        self.filename = filename
        self.lastspawn = lastspawn
        self.proc = None
        self.nextkill = 0
        self.killstate = 0
        self.dead = False
        self.mtime = mtime
        self.generation = GENERATION
        self.buffer = ''
        self.datalines = []
        self.last_datapoint = int(time.time())
        LOG.debug('Construct a Collector object')

    def read(self):
        """Read bytes from subprocess and store them in self.datalines"""

        # read stderr for log messages
        try:
            out = self.proc.stderr.read()
            if out:
                for line in out.splitlines():
                    LOG.warning('stderr %s: %s', self.name, line)
        except IOError as err:
            if err.errno != errno.EAGAIN:
                raise
        except:
            LOG.exception('uncaught exception in stderr read: %s', self.name)

        # use a buffer
        try:
            self.buffer += self.proc.stdout.read()
            if len(self.buffer):
                LOG.debug('reading [%s], buffer now %d bytes',
                          self.name, len(self.buffer))
        except IOError as err:
            if err.errno != errno.EAGAIN:
                raise
        except AttributeError:
            # sometimes the process goes away in another thread and we don't
            # have it anymore, so log an error and bail
            LOG.exception('caught exception, collector process went ' +
                          'away while reading stdout')
        except:
            LOG.exception('uncaught exception in stdout read')
            return

        # iterate for each line we have
        while self.buffer:
            idx = self.buffer.find('\n')
            if idx == -1:
                break

            # one full line is now found and pull it out of the buffer
            line = self.buffer[0:idx].strip()
            if line:
                self.datalines.append(line)
                self.last_datapoint = int(time.time())
				
            self.buffer = self.buffer[idx+1:]

    def collect(self):
        """Reads input from the collector and returns the lines. This is
           a generator that returns a line as it becomes available."""

        while self.proc is not None:
            self.read()
            if not len(self.datalines):
                return
            while len(self.datalines):
                yield self.datalines.pop(0)

    def shutdown(self):
        """Cleanly shut down the collector"""

        LOG.debug('Shutting down: %s', self.name)
        if not self.proc:
            return
        try:
            if self.proc.poll() is None:
                kill(self.proc)
                for attempt in range(5):
                    if self.proc.poll() is not None:
                        return
                    LOG.info('Waiting %ds for PID %d (%s) to exit...'
                             % (5 - attempt, self.proc.pid, self.name))
                    time.sleep(1)
                kill(self.proc, signal.SIGKILL)
                self.proc.wait()
        except:
            # we really don't want to die as we're trying to exit gracefully
            LOG.exception('ignoring uncaught exception while shutting down')


class StdinCollector(Collector):
    """A StdinCollector simply reads from STDIN and provides the
       data.  This collector presents a uniform interface for the
       ReaderThread."""

    def __init__(self):
        super(StdinCollector, self).__init__('stdin', 0, '<stdin>')
        self.proc = True

    def read(self):
        """Read lines from STDIN and store them."""

        global ALIVE
        line = sys.stdin.readline()
        if line:
            self.datalines.append(line.rstrip())
        else:
            ALIVE = False

    def shutdown(self):
        pass


class ReaderThread(threading.Thread):
    """The main ReaderThread is responsible for reading from the collectors.
       All data read is put into the self.readerq Queue, which is consumed 
	   by the SenderThread."""

    def __init__(self):
        """Constructor."""

        super(ReaderThread, self).__init__()
        self.readerq = ReaderQueue(MAX_READQ_SIZE)
        self.name = 'Reader'

    def run(self):
        """Main loop for this thread. Reads from collectors,
           does data processing and puts the data into the queue."""

        LOG.info("ReaderThread is up and running")

        while ALIVE:
            for col in all_living_collectors():
                for line in col.collect():
                    self.process_line(col, line)

            time.sleep(1)

    def process_line(self, col, line):
        """Parses the given line and appends the result to the reader queue."""

        data_list = line.split()
		
        #Only 'host', 'key', 'value', <'timestampe'> are needed.
        if len(data_list) > 4 or len(data_list) < 3:
            LOG.warning('%s lenth of line %s is %d, invalid', 
                        col.name, line, len(data_list))
            return
        else:
            self.readerq.nput(data_list)
            LOG.debug('Put into Queue: %s', str(data_list))


class SenderThread(threading.Thread):
    """The SenderThread is responsible for connecting to the OpsMonitor
       and sending the data to it."""

    def __init__(self, reader, dryrun,server_ip, server_port=DEFAULT_PORT, stdin_mode=False):
        """Constructor.

        Args:
          reader: A reference to a ReaderThread instance.
          dryrun: If true, data points will be printed on stdout instead of
                  being sent to the OpsMonitor.
        """
        super(SenderThread, self).__init__()
        self.name = 'Sender'
        self.dryrun = dryrun
        self.reader = reader
        self.server_ip = server_ip  # The current OpsMonitor host we've selected.
        self.server_port = server_port  # The port of the current OpsMonitor.
        self.sendq = []
        self.stdin_mode = stdin_mode

    def run(self):
        """Main loop. A simple scheduler. Loop waiting for 5 seconds for data on
           the queue. If there is data, wait 5 more seconds and grab all of the
           pending data and send it."""

        errors = 0  # How many uncaught exceptions in a row we got.
        while ALIVE:
            try:
                try:
                    line = self.reader.readerq.get(True, 5)
                    LOG.debug('Get line from readerq: %s', line)
                except Empty:
                    continue
                self.sendq.append(line)
                LOG.debug('Waiting 5 seconds for more data')
                if not self.stdin_mode:
                    time.sleep(5)  # Wait for more data
                while True:
                    # prevents self.sendq fast growing in case of sending fails
                    # in send_data()
                    if len(self.sendq) > MAX_SENDQ_SIZE:
                        LOG.warn('sendq is over %d', MAX_SENDQ_SIZE)
                        break
                    try:
                        line = self.reader.readerq.get(False)
                        LOG.debug('Get line from readerq: %s', line)
                    except Empty:
                        break
                    self.sendq.append(line)

                if ALIVE or self.stdin_mode:
                    self.send_data()
                errors = 0  # We managed to do a successful iteration.
            except (ArithmeticError, EOFError, EnvironmentError, LookupError,
                    ValueError), e:
                errors += 1
                if errors > MAX_UNCAUGHT_EXCEPTIONS:
                    shutdown()
                    raise
                LOG.exception('Uncaught exception in SenderThread, ignoring')
                time.sleep(1)
                continue
            except:
                LOG.exception('Uncaught exception in SenderThread, going to exit')
                shutdown()
                raise

    def send_data(self):
        """Sends outstanding data in self.sendq to the OpsMonitor in one operation."""

        # construct the output string
        metrics = []
        for data_list in self.sendq:
            m = ZabbixMetric(*data_list)
            metrics.append(m)

        if not metrics:
            LOG.debug('send_data no data')
            return

        # try sending our data.  if an exception occurs, just error and
        # try sending again next time.
        
        if self.dryrun:
            sys.stdout.write(str(metrics) + '\n')
            return
        else:
            sender = ZabbixSender(self.server_ip, self.server_port)
            if sender.send(metrics):
                self.sendq = []


class ZabbixMetric(object):
    """ZabbixMetric contain one metric for zabbix server.

    Args:
      host: Hostname as it displayed in Zabbix.
      key: Key by which you will identify this metric.
      value: Metric value.
      clock: Unix timestamp. Current time will used if not specified.

    >>> from pyzabbix import ZabbixMetric
    >>> ZabbixMetric('localhost', 'cpu[usage]', 20)
    """

    def __init__(self, host, key, value, clock=None):
        self.host = str(host)
        self.key = str(key)
        self.value = str(value)
        if clock:
            self.clock = clock
        else:
            self.clock = int(time.time())
			
    def __repr__(self):
        """Represent detailed ZabbixMetric view."""

        result = json.dumps(self.__dict__)

        return result


class ZabbixSender(object):
    """ZabbixSender send metrics to Zabbix server.

    Args:
      server_ip: Zabbix server ip address. Default: `127.0.0.1`
      server_port: Zabbix server port. Default: `10051`

    >>> import ZabbixMetric, ZabbixSender
    >>> metrics = []
    >>> m = ZabbixMetric('localhost', 'cpu[usage]', 20)
    >>> metrics.append(m)
    >>> zbx = ZabbixSender('127.0.0.1')
    >>> zbx.send(metric)
    """

    def __init__(self, server_ip='127.0.0.1', server_port=10051):
        self.server_ip = server_ip
        self.server_port = server_port

    def __repr__(self):
        """Represent detailed ZabbixSender view."""

        result = json.dumps(self.__dict__)

        return result	
	
    def _receive(self, sock, count):
        """Reads socket to receive data from zabbix server.

        Args:
          socket: Socket to read.
          count: Number of bytes to read from socket.
        """

        buf = b''

        while len(buf) < count:
            chunk = sock.recv(count - len(buf))
            if not chunk:
                break
            buf += chunk

        return buf

    def create_messages(self, metrics):
        """Create a list of zabbix messages from a list of ZabbixMetrics.

        Args:
          metrics_array: List of :class:`zabbix.sender.ZabbixMetric`.
          return: List of zabbix messages.
        """

        messages = []

        # Fill the list of messages
        for m in metrics:
            messages.append(str(m))

        return messages

    def create_request(self, messages):
        """Create a formatted request to zabbix from a list of messages.

        Args:
          messages: List of zabbix messages
          return: Formatted zabbix request
        """

        msg = ','.join(messages)
        request = '{{"request":"sender data","data":[{msg}]}}'.format(msg=msg)
        request = request.encode("utf-8")

        return request

    def create_packet(self, request):
        """Create a formatted packet from a request.

        Args:
          request: Formatted zabbix request
          return: Data packet for zabbix
        """

        data_len = struct.pack('<Q', len(request))
        packet = b'ZBXD\x01' + data_len + request

        return packet

    def get_response(self, connection):
        """Get response from zabbix server, reads from self.socket.

        Args:
          connection: Socket to read.
          return: Response from zabbix server or False in case of error.
        """

        response_header = self._receive(connection, 13)

        if (not response_header.startswith(b'ZBXD\x01')
                or len(response_header) != 13):
            LOG.error('Zabbix return not valid response.')
            result = False
        else:
            response_len = struct.unpack('<Q', response_header[5:])[0]
            response_body = connection.recv(response_len)
            result = json.loads(response_body.decode("utf-8"))
            LOG.info('Data received: %s', result)

        return result

    def send(self, metrics):
        """Send the metrics to zabbix server.

        Args:type metrics: list
          metrics: List of ZabbixMetric to send to Zabbix
          return: `True` if messages was sent successful, else `False`.
        """

        messages = self.create_messages(metrics)
        request = self.create_request(messages)
        packet = self.create_packet(request)

        try:
            connection = socket.socket()
            connection.settimeout(5)
            connection.connect((self.server_ip, self.server_port))
            connection.sendall(packet)
        except Exception as err:
            try:
                connection.close()
            except Exception as err:
                pass
            raise Exception(err)

        response = self.get_response(connection)

        if response and response.get('response') == 'success':
            return True
        else:
            return False
		

def setup_logging(logfile=DEFAULT_LOG, max_bytes=None, backup_count=None):
    """Sets up logging and associated handlers."""

    LOG.setLevel(logging.INFO)
    if backup_count is not None and max_bytes is not None:
        assert backup_count > 0
        assert max_bytes > 0
        ch = RotatingFileHandler(logfile, 'a', max_bytes, backup_count)
    else:  # Setup stream handler.
        ch = logging.StreamHandler(sys.stdout)

    ch.setFormatter(logging.Formatter('%(asctime)s %(name)s[%(process)d]'
              '[%(threadName)s]%(funcName)s %(levelname)s: %(message)s'))
    LOG.addHandler(ch)
	

def parse_cmdline(argv):
    """Parses the command-line."""

    try:
        from collectors.etc import config
        defaults = config.get_defaults()
    except ImportError:
        sys.stderr.write("ImportError: Could not load defaults from configuration. Using hardcoded values\n")
        default_cdir = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), 'collectors')
        defaults = {
            'verbose': False,
            'allowed_inactivity_time': 600,
            'dryrun': False,
            'max_bytes': 64 * 1024 * 1024,
            'server_ip': '192.168.1.102',
            'port': 10051,
            'pidfile': '/var/run/omcollector.pid',
            'remove_inactive_collectors': False,
            'backup_count': 1,
            'logfile': '/var/log/omcollector.log',
            'cdir': default_cdir,
            'stdin': False,
            'daemonize': False
        }
    except:
        sys.stderr.write("Unexpected error: %s\n" % sys.exc_info()[0])
        raise

    # get arguments
    parser = OptionParser(description='Manages collectors which gather '
                                       'data and report back.')
    parser.add_option('-c', '--collector-dir', dest='cdir', metavar='DIR',
                        default=defaults['cdir'],
                        help='Directory where the collectors are located.')
    parser.add_option('-d', '--dry-run', dest='dryrun', action='store_true',
                        default=defaults['dryrun'],
                        help='Don\'t actually send anything to the OpsMonitor, '
                           'just print the datapoints.')
    parser.add_option('-D', '--daemonize', dest='daemonize', action='store_true',
                        default=defaults['daemonize'],
                        help='Run as a background daemon.')
    parser.add_option('-z', '--server_ip', dest='server_ip',
                        default=defaults['server_ip'],
                        help='Hostname to use to connect to the OpsMonitor.')
    parser.add_option('-S', '--stdin', dest='stdin', action='store_true',
                        default=defaults['stdin'],
                        help='Run once, read data points from stdin.')
    parser.add_option('-p', '--port', dest='port', type='int',
                        default=defaults['port'], metavar='PORT',
                        help='Port to connect to the OpsMonitor instance on. '
                        'default=%default')
    parser.add_option('-v', dest='verbose', action='store_true',
                        default=defaults['verbose'],
                        help='Verbose mode (log debug messages).')
    parser.add_option('-P', '--pidfile', dest='pidfile',
                        default=defaults['pidfile'],
                        metavar='FILE', help='Write our pidfile')
    parser.add_option('--allowed-inactivity-time', dest='allowed_inactivity_time', type='int',
                        default=ALLOWED_INACTIVITY_TIME, metavar='ALLOWEDINACTIVITYTIME',
                            help='How long to wait for datapoints before assuming '
                                'a collector is dead and restart it. '
                                'default=%default')
    parser.add_option('--remove-inactive-collectors', dest='remove_inactive_collectors', action='store_true',
                        default=defaults['remove_inactive_collectors'], help='Remove collectors not sending data '
                                          'in the max allowed inactivity interval')
    parser.add_option('--max-bytes', dest='max_bytes', type='int',
                        default=defaults['max_bytes'],
                        help='Maximum bytes per a logfile.')
    parser.add_option('--backup-count', dest='backup_count', type='int',
                        default=defaults['backup_count'], help='Maximum number of logfiles to backup.')
    parser.add_option('--logfile', dest='logfile', type='str',
                        default=DEFAULT_LOG,
                        help='Filename where logs are written to.')
    (options, args) = parser.parse_args(args=argv[1:])
    if not options.server_ip:
        parser.error('--server_ip must be specified')
    # We cannot write to stdout when we're a daemon.
    if (options.daemonize or options.max_bytes) and not options.backup_count:
        options.backup_count = 1
    return (options, args)


def daemonize():
    """Performs the necessary dance to become a background daemon."""
    if os.fork():
        os._exit(0)
    os.chdir("/")
    os.umask(022)
    os.setsid()
    os.umask(0)
    if os.fork():
        os._exit(0)
    stdin = open(os.devnull)
    stdout = open(os.devnull, 'w')
    os.dup2(stdin.fileno(), 0)
    os.dup2(stdout.fileno(), 1)
    os.dup2(stdout.fileno(), 2)
    stdin.close()
    stdout.close()
    os.umask(022)
    for fd in xrange(3, 1024):
        try:
            os.close(fd)
        except OSError:
            pass


def setup_python_path(collector_dir):
    """Sets up PYTHONPATH so that collectors can easily import common code."""
    mydir = os.path.dirname(collector_dir)
    libdir = os.path.join(mydir, 'collectors', 'lib')
    if not os.path.isdir(libdir):
        return
    pythonpath = os.environ.get('PYTHONPATH', '')
    if pythonpath:
        pythonpath += ':'
    pythonpath += mydir
    os.environ['PYTHONPATH'] = pythonpath
    LOG.debug('Set PYTHONPATH to %r', pythonpath)


def main(argv):
    """The main omcollector entry point and loop."""

    try:
        options, args = parse_cmdline(argv)
    except:
        sys.stderr.write("Unexpected error: %s\n" % sys.exc_info()[0])
        return 1

    if options.daemonize:
        daemonize()
    setup_logging(options.logfile, options.max_bytes or None,
                  options.backup_count or None)

    if options.verbose:
        LOG.setLevel(logging.DEBUG)

    if options.pidfile:
        write_pid(options.pidfile)

    # validate everything
    options.cdir = os.path.realpath(options.cdir)
    if options.daemonize and not os.path.isdir(options.cdir):
        LOG.fatal('No such directory: %s', options.cdir)
        return 1
    modules = load_etc_dir(options)

    setup_python_path(options.cdir)

    # gracefully handle death for normal termination paths and abnormal
    atexit.register(shutdown)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, shutdown_signal)

    # at this point we're ready to start processing, so start the ReaderThread
    # so we can have it running and pulling in data for us
    reader = ReaderThread()
    reader.start()
	
    # and setup the sender to start writing out to the conn
    sender = SenderThread(reader, options.dryrun,
                          options.server_ip, options.port, options.stdin)
    sender.start()
    LOG.info('SenderThread startup complete')

    # if we're in stdin mode, build a stdin collector and just join on the
    # reader thread since there's nothing else for us to do here
    if options.stdin:
        register_collector(StdinCollector())
#        stdin_loop(options, modules, sender)
    else:
        sys.stdin.close()
        main_loop(options, modules, sender)

    # We're exiting, make sure we don't leave any collector behind.
    for col in all_living_collectors():
        col.shutdown()
    LOG.debug('Shutting down -- joining the reader thread.')
    reader.join()
    LOG.debug('Shutting down -- joining the sender thread.')
    sender.join()

def stdin_loop(options, modules, sender):
    """The main loop of the program that runs when we are in stdin mode."""

    global ALIVE
    next_heartbeat = int(time.time() + 600)
    while ALIVE:
        time.sleep(5)
        reload_changed_config_modules(modules, options, sender)
        now = int(time.time())
        if now >= next_heartbeat:
            LOG.info('Heartbeat (%d collectors running)'
                     % sum(1 for col in all_living_collectors()))
            next_heartbeat = now + 600

def main_loop(options, modules, sender):
    """The main loop of the program that runs when we're not in stdin mode."""

    next_heartbeat = int(time.time() + 600)
    while ALIVE:
        populate_collectors(options.cdir)
        reload_changed_config_modules(modules, options, sender)
        reap_children()
        check_children(options)
        spawn_children()
        time.sleep(15)
        now = int(time.time())
        if now >= next_heartbeat:
            LOG.info('Heartbeat (%d collectors running)'
                     % sum(1 for col in all_living_collectors()))
            next_heartbeat = now + 600


def list_config_modules(etcdir):
    """Returns an iterator that yields the name of all the config modules."""
    if not os.path.isdir(etcdir):
        return iter(())  # Empty iterator.
    return (name for name in os.listdir(etcdir)
            if (name.endswith('.py')
                and os.path.isfile(os.path.join(etcdir, name))))


def load_etc_dir(options):
    """Loads any Python module from omcollector's own 'etc' directory.

    Returns: A dict of path -> (module, timestamp).
    """

    etcdir = os.path.join(options.cdir, 'etc')
    sys.path.append(etcdir)  # So we can import modules from the etc dir.
    modules = {}  # path -> (module, timestamp)
    for name in list_config_modules(etcdir):
        path = os.path.join(etcdir, name)
        module = load_config_module(name, options)
        modules[path] = (module, os.path.getmtime(path))
    return modules


def load_config_module(name, options):
    """Imports the config module of the given name

    The 'name' argument can be a string, in which case the module will be
    loaded by name, or it can be a module object, in which case the module
    will get reloaded.

    If the module has an 'onload' function, calls it.
    Returns: the reference to the module loaded.
    """

    if isinstance(name, str):
        LOG.info('Loading %s', name)
        d = {}
        # Strip the trailing .py
        module = __import__(name[:-3], d, d)
    else:
        module = reload(name)
 
    return module


def reload_changed_config_modules(modules, options, sender):
    """Reloads any changed modules from the 'etc' directory.

    Args:
      cdir: The path to the 'collectors' directory.
      modules: A dict of path -> (module, timestamp).
    Returns: whether or not anything has changed.
    """

    etcdir = os.path.join(options.cdir, 'etc')
    current_modules = set(list_config_modules(etcdir))
    current_paths = set(os.path.join(etcdir, name)
                        for name in current_modules)
    changed = False

    # Reload any module that has changed.
    for path, (module, timestamp) in modules.iteritems():
        if path not in current_paths:  # Module was removed.
            continue
        mtime = os.path.getmtime(path)
        if mtime > timestamp:
            LOG.info('Reloading %s, file has changed', path)
            module = load_config_module(module, options)
            modules[path] = (module, mtime)
            changed = True

    # Remove any module that has been removed.
    for path in set(modules).difference(current_paths):
        LOG.info('%s has been removed, omcollector should be restarted', path)
        del modules[path]
        changed = True

    # Check for any modules that may have been added.
    for name in current_modules:
        path = os.path.join(etcdir, name)
        if path not in modules:
            module = load_config_module(name, options)
            modules[path] = (module, os.path.getmtime(path))
            changed = True

    return changed


def write_pid(pidfile):
    """Write our pid to a pidfile."""
    f = open(pidfile, "w")
    try:
        f.write(str(os.getpid()))
    finally:
        f.close()


def all_collectors():
    """Generator to return all collectors."""

    return COLLECTORS.itervalues()


# collectors that are not marked dead
def all_valid_collectors():
    """Generator to return all defined collectors that haven't been marked
       dead in the past hour, allowing temporarily broken collectors a
       chance at redemption."""

    now = int(time.time())
    for col in all_collectors():
        if not col.dead or (now - col.lastspawn > 3600):
            yield col


# collectors that have a process attached (currenty alive)
def all_living_collectors():
    """Generator to return all defined collectors that have
       an active process."""

    for col in all_collectors():
        if col.proc is not None:
            yield col


def shutdown_signal(signum, frame):
    """Called when we get a signal and need to terminate."""
    LOG.warning("shutting down, got signal %d", signum)
    shutdown()


def kill(proc, signum=signal.SIGTERM):
  os.killpg(proc.pid, signum)


def shutdown():
    """Called by atexit and when we receive a signal, this ensures we properly
       terminate any outstanding children."""

    global ALIVE
    # prevent repeated calls
    if not ALIVE:
        return
    # notify threads of program termination
    ALIVE = False

    LOG.info('shutting down children')

    # tell everyone to die
    for col in all_living_collectors():
        col.shutdown()

    LOG.info('exiting')
    sys.exit(1)


def reap_children():
    """When a child process dies, we have to determine why it died and whether
       or not we need to restart it.  This method manages that logic."""

    for col in all_living_collectors():
        now = int(time.time())
        # FIXME: this is not robust.  the asyncproc module joins on the
        # reader threads when you wait if that process has died.  this can cause
        # slow dying processes to hold up the main loop.  good for now though.
        status = col.proc.poll()
        if status is None:
            continue
        col.proc = None

        # behavior based on status.  a code 0 is normal termination, code 13
        # is used to indicate that we don't want to restart this collector.
        # any other status code is an error and is logged.
        if status == 13:
            LOG.info('removing %s from the list of collectors (by request)',
                      col.name)
            col.dead = True
        elif status != 0:
            LOG.warning('collector %s terminated after %d seconds with '
                        'status code %d, marking dead',
                        col.name, now - col.lastspawn, status)
            col.dead = True
        else:
            register_collector(Collector(col.name, col.interval, col.filename,
                                         col.mtime, col.lastspawn))

def check_children(options):
    """When a child process hasn't received a datapoint in a while,
       assume it's died in some fashion and restart it."""

    for col in all_living_collectors():
        now = int(time.time())

        if col.last_datapoint < (now - options.allowed_inactivity_time):
            # It's too old, kill it
            LOG.warning('Terminating collector %s after %d seconds of inactivity',
                        col.name, now - col.last_datapoint)
            col.shutdown()
            if not options.remove_inactive_collectors:
                register_collector(Collector(col.name, col.interval, col.filename,
                                             col.mtime, col.lastspawn))


def set_nonblocking(fd):
    """Sets the given file descriptor to non-blocking mode."""
    fl = fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK
    fcntl.fcntl(fd, fcntl.F_SETFL, fl)


def spawn_collector(col):
    """Takes a Collector object and creates a process for it."""

    LOG.info('%s (interval=%d) needs to be spawned', col.name, col.interval)

    # FIXME: do custom integration of Python scripts into memory/threads
    # if re.search('\.py$', col.name) is not None:
    #     ... load the py module directly instead of using a subprocess ...
    try:
        col.proc = subprocess.Popen(col.filename, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    close_fds=True,
                                    preexec_fn=os.setsid)
    except OSError, e:
        LOG.error('Failed to spawn collector %s: %s' % (col.filename, e))
        return
    # The following line needs to move below this line because it is used in
    # other logic and it makes no sense to update the last spawn time if the
    # collector didn't actually start.
    col.lastspawn = int(time.time())
    # Without setting last_datapoint here, a long running check (>15s) will be 
    # killed by check_children() the first time check_children is called.
    col.last_datapoint = col.lastspawn
    set_nonblocking(col.proc.stdout.fileno())
    set_nonblocking(col.proc.stderr.fileno())
    if col.proc.pid > 0:
        col.dead = False
        LOG.info('spawned %s (pid=%d)', col.name, col.proc.pid)
        return
    # FIXME: handle errors better
    LOG.error('failed to spawn collector: %s', col.filename)


def spawn_children():
    """Iterates over our defined collectors and performs the logic to
       determine if we need to spawn, kill, or otherwise take some
       action on them."""

    if not ALIVE:
        return

    for col in all_valid_collectors():
        now = int(time.time())
        if col.interval == 0:
            if col.proc is None:
                spawn_collector(col)
        elif col.interval <= now - col.lastspawn:
            if col.proc is None:
                spawn_collector(col)
                continue

            # I'm not very satisfied with this path.  It seems fragile and
            # overly complex, maybe we should just reply on the asyncproc
            # terminate method, but that would make the main omcollector
            # block until it dies... :|
            if col.nextkill > now:
                continue
            if col.killstate == 0:
                LOG.warning('warning: %s (interval=%d, pid=%d) overstayed '
                            'its welcome, SIGTERM sent',
                            col.name, col.interval, col.proc.pid)
                kill(col.proc)
                col.nextkill = now + 5
                col.killstate = 1
            elif col.killstate == 1:
                LOG.error('error: %s (interval=%d, pid=%d) still not dead, '
                           'SIGKILL sent',
                           col.name, col.interval, col.proc.pid)
                kill(col.proc, signal.SIGKILL)
                col.nextkill = now + 5
                col.killstate = 2
            else:
                LOG.error('error: %s (interval=%d, pid=%d) needs manual '
                           'intervention to kill it',
                           col.name, col.interval, col.proc.pid)
                col.nextkill = now + 300


def populate_collectors(coldir):
    """Maintains our internal list of valid collectors.  This walks the
       collector directory and looks for files.  In subsequent calls, this
       also looks for changes to the files -- new, removed, or updated files,
       and takes the right action to bring the state of our running processes
       in line with the filesystem."""

    global GENERATION
    GENERATION += 1

    # get numerics from scriptdir, we're only setup to handle numeric paths
    # which define intervals for our monitoring scripts
    for interval in os.listdir(coldir):
        if not interval.isdigit():
            continue
        interval = int(interval)

        for colname in os.listdir('%s/%d' % (coldir, interval)):
            if colname.startswith('.'):
                continue

            filename = '%s/%d/%s' % (coldir, interval, colname)
            if os.path.isfile(filename) and os.access(filename, os.X_OK):
                mtime = os.path.getmtime(filename)

                # if this collector is already 'known', then check if it's
                # been updated (new mtime) so we can kill off the old one
                # (but only if it's interval 0, else we'll just get
                # it next time it runs)
                if colname in COLLECTORS:
                    col = COLLECTORS[colname]

                    # if we get a dupe, then ignore the one we're trying to
                    # add now.  there is probably a more robust way of doing
                    # this...
                    if col.interval != interval:
                        LOG.error('two collectors with the same name %s and '
                                   'different intervals %d and %d',
                                   colname, interval, col.interval)
                        continue

                    # we have to increase the generation or we will kill
                    # this script again
                    col.generation = GENERATION
                    if col.mtime < mtime:
                        LOG.info('%s has been updated on disk', col.name)
                        col.mtime = mtime
                        if not col.interval:
                            col.shutdown()
                            LOG.info('Respawning %s', col.name)
                            register_collector(Collector(colname, interval,
                                                         filename, mtime))
                else:
                    register_collector(Collector(colname, interval, filename,
                                                 mtime))

    # now iterate over everybody and look for old generations
    to_delete = []
    for col in all_collectors():
        if col.generation < GENERATION:
            LOG.info('collector %s removed from the filesystem, forgetting',
                      col.name)
            col.shutdown()
            to_delete.append(col.name)
    for name in to_delete:
        del COLLECTORS[name]


if __name__ == '__main__':
    main(sys.argv)


