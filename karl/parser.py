#! /usr/bin/env python

import argparse
import errno
import glob
import os
import readline
import signal
import sqlite3
import sys
from cmd import Cmd
from timeit import default_timer as timer

db = None  # Global db


class MyParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        raise SystemExit(2)


class MyCmd(Cmd):
    prompt = '>'

    def do_QUERY(self, args):
        """
        Execute queries on log files.

        Format: IP CPUID START_DATE START_TIME END_DATE END_TIME

        Syntax: QUERY IP CPUID YYYY-MM-DD HH:MM YYYY-MM-DD HH:MM
        """
        query = '''
            SELECT strftime('%Y-%m-%d %H:%M', datetime(unix_time, 'unixepoch')),
            usage FROM log WHERE ip = ? AND cpuid = ? AND unix_time
            BETWEEN strftime('%s', ?) AND strftime('%s', ?) ORDER BY unix_time
        '''
        format_result = {
            'fmt': '({0[0]}, {0[1]}%)',
            'sep': ', '
        }

        try:
            args = args.rsplit(' ', 6)
            if len(args) != 6:
                raise ValueError
            query_args = (
                args[0], args[1], ' '.join(args[2:4]), ' '.join(args[4:6]),
            )
            output = query_logs(query, query_args, format_result)
            print output

        except ValueError as e:
            print 'Invalid query'

    def do_LOAD(self, args):
        """
        Calculate average load.

        Format: IP CPUID START_DATE START_TIME END_DATE END_TIME

        Syntax: LOAD IP CPUID YYYY-MM-DD HH:MM YYYY-MM-DD HH:MM
        """
        query = '''
            SELECT avg(usage) FROM log WHERE ip = ? AND cpuid = ? AND unix_time
            BETWEEN strftime('%s', ?) AND strftime('%s', ?) ORDER BY unix_time
        '''

        format_result = {
            'fmt': '{0[0]}%',
            'sep': ''
        }

        try:
            args = args.rsplit(' ', 6)
            if len(args) != 6:
                raise ValueError
            query_args = (
                args[0], args[1], ' '.join(args[2:4]), ' '.join(args[4:6]),
            )
            output = query_logs(query, query_args, format_result)
            print output

        except ValueError as e:
            print 'Invalid query'

    def do_STAT(self, args):
        """
        Show load stats per cpu.

        Format: IP START_DATE START_TIME END_DATE END_TIME

        Syntax: STAT IP YYYY-MM-DD HH:MM YYYY-MM-DD HH:MM
        """
        query = '''
            SELECT cpuid, avg(usage) FROM log WHERE ip = ? AND unix_time BETWEEN
            strftime('%s', ?) AND strftime('%s', ?) GROUP BY cpuid ORDER BY unix_time
        '''

        format_result = {
            'fmt': '{0[0]}: {0[1]}%',
            'sep': '\n'
        }

        try:
            args = args.rsplit(' ', 5)
            if len(args) != 5:
                raise ValueError
            query_args = (
                args[0], ' '.join(args[1:3]), ' '.join(args[3:5]),
            )
            output = query_logs(query, query_args, format_result)
            print output

        except ValueError as e:
            print 'Invalid query'

    def do_EXIT(self, args):
        """
        Exit from application.

        Syntax: EXIT
        """
        return True

    def emptyline(self):
        pass


def exit_gracefully(signum, frame):
    raise SystemExit()


def parse_cli_args():
    parser = MyParser()
    parser.add_argument('path', help='PATH to data directory')
    args = parser.parse_args()
    path = args.path

    return path


def db_init():
    dbconn = sqlite3.connect(':memory:')

    dbconn.executescript('''
        CREATE TABLE IF NOT EXISTS log(
            unix_time INT, ip TEXT, cpuid INT, usage INT
        );
        CREATE INDEX IF NOT EXISTS index_all on log (ip,unix_time,cpuid);
    ''')
    dbconn.commit()
    return dbconn


def load_logs(data_dir):
    log_files = glob.glob(os.path.join(data_dir, '*'))
    if not log_files:
        print 'No log files found in ' + data_dir
        raise SystemExit()

    dbconn = db_init()

    for log in log_files:
        try:
            with open(log) as f:
                data = [tuple(line.strip().split(' ')) for line in f]
                dbconn.executemany('INSERT INTO log VALUES (?,?,?,?)', data)
                dbconn.commit()
        except IOError as e:
            if e.errno != errno.EISDIR:
                raise
    return dbconn


def query_logs(query, query_args, format_result):
    start = timer()  # For query time measurement
    result = db.execute(query, query_args).fetchall()
    if not result:
        output = 'No data'
    else:
        output = format_result['sep'].join(
            [format_result['fmt'].format(p) for p in result]
            )
    end = timer()  # For query time measurement
    print '\n' + str(end - start) + '\n'  # For query time measurement

    return output


def main():
    try:
        global db
        signal.signal(signal.SIGINT, exit_gracefully)
        data_dir = parse_cli_args()
        db = load_logs(data_dir)
        app = MyCmd()
        app.cmdloop()
    finally:
        if db is not None:
            db.close()


if __name__ == '__main__':
    main()
