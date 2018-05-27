#! /usr/bin/env python

import argparse
import os
import signal
import sys
from datetime import datetime, time, timedelta
from random import randint

from faker import Faker


class MyParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        raise SystemExit(2)


def exit_gracefully(signum, frame):
    raise SystemExit()


def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta


parser = MyParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('path', help='Path to data directory')
parser.add_argument('-q', metavar=('INT'), default='10',
                    type=int, help='IPs quantity')
parser.add_argument('-s', '--seed', default='FortyTwo', help='Random seed')
args = parser.parse_args()

fake = Faker()
fake.seed(args.seed)

data_dir = args.path
ip_quantity = args.q
date = datetime(2014, 10, 31)
delta = timedelta(minutes=1)
start_datetime = datetime.combine(date, time.min)
end_datetime = datetime.combine(date, time.max)

signal.signal(signal.SIGINT, exit_gracefully)

for _ in range(ip_quantity):
    ip = fake.ipv4_private(network=False, address_class=None)
    log_file = os.path.join(data_dir, ip + '.log')
    try:
        with open(log_file, 'w') as log:
            for dt in datetime_range(start_datetime, end_datetime, delta):
                unix_time = int((dt - datetime(1970, 1, 1)).total_seconds())

                for cpu in range(2):
                    usage = randint(0, 100)
                    line = [unix_time, ip, cpu, usage]
                    log.write(" ".join(map(str, line)) + '\n')
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
        break
