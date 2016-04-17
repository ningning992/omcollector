#!/usr/bin/env python

import time

host = 'Zabbix_server'
key = 'test.key'

value = int(time.time()%100)

print host,key,value
