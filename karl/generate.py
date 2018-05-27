#! /usr/bin/env python

import argparse
import os
import signal
import sys
from datetime import datetime, time, timedelta
from random import randint

from faker import Faker


def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta


def exit_gracefully(signum, frame):
    print '\nExiting...'
    sys.exit(0)


parser = argparse.ArgumentParser()
parser.add_argument('path', help='PATH to data directory')
args = parser.parse_args()

fake = Faker()
fake.seed('FortyTwo')

data_dir = args.path
date = datetime(2014, 10, 31)
delta = timedelta(minutes=1)
start_datetime = datetime.combine(date, time.min)
end_datetime = datetime.combine(date, time.max)

signal.signal(signal.SIGINT, exit_gracefully)

for _ in range(10):
    ip = fake.ipv4_private(network=False, address_class=None)
    log_file = os.path.join(data_dir, ip + '.log')
    try:
        with open(log_file, 'w') as log:

            for dt in datetime_range(start_datetime, end_datetime, delta):
                unix_time = dt.strftime('%s')

                for cpu in range(2):
                    usage = randint(0, 100)
                    line = [unix_time, ip, cpu, usage]
                    log.write(" ".join(map(str, line)) + '\n')
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
        break