#!/usr/bin/env python

import time
import random
import sys

while True:
    print 'Zabbix_server','zero.key',random.randint(40,80)
    sys.stdout.flush()
    time.sleep(30)
    
