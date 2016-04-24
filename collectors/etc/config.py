#!/usr/bin/python

import os
import sys


def get_defaults():
    """Configuration values to use as defaults in the code

        This is called by the OptionParser.
    """

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

    return defaults

