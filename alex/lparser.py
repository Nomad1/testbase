# -*- coding: utf-8 -*-
import glob
import os
import readline
import sqlite3
import sys
import time
from datetime import datetime

from progress.bar import Bar

# db = sqlite3.connect('mydb.db')
db = sqlite3.connect(':memory:')
cursor = db.cursor()


def list_dir():
    try:
        path, ext = sys.argv[1], sys.argv[2]
        # print(path, ext)
        if path:
            if os.path.isdir(path):
                fl = glob.glob('{}/*{}'.format(path, ext))
                if fl:
                    return fl
                else:
                    print('{} is wrong extension.'.format(ext))
            else:
                print('{} wrong PATH'.format(path))
    except Exception as perr:
        print('Please, specify the path to the folder with log files and file extension!')
        print('For example: ./lparser.py /var/log/<some_folder> log')
        return False


flist = list_dir()


class Completer(object):

    def __init__(self, options):
        self.options = sorted(options)
        return

    def complete(self, text, state):
        response = None
        if state == 0:
            # This is the first time for this text, so build a match list.
            if text:
                self.matches = [s for s in self.options if s and s.startswith(text)]
            else:
                self.matches = self.options[:]

        # Return the state'th item from the match list,
        # if we have that many.
        try:
            response = self.matches[state]
        except IndexError:
            response = None
        return response


def update_data():

    rawsql = """DROP TABLE IF EXISTS Logs;
    CREATE TABLE Logs(id INTEGER PRIMARY KEY, time TEXT, Ip varchar(20, 0), Cpu int, Load int);
    """
    cursor.executescript(rawsql)

    bar = Bar('Update info:', max=len(flist))
    for fname in flist:
        try:
            with open(fname, 'r') as fdata:
                lines = fdata.readlines()
                data = [tuple(line.strip().split()) for line in lines]
                db.executemany("INSERT INTO Logs (time, ip, cpu, load) VALUES (?,?,?,?);", data)
                bar.next()
        except Exception as serror:
            print('Error for {} file format.'.format(fname))
            print('Format must be 1526796060 1.235.153.220 0 49 (one row for one line)')
            break
    db.commit()
    bar.finish()


def help(hp=False):
    hptxt = {
        'help': """
    Supported commands:

    REFRESH - for refresh data
    """,
        'query': """
    QUERY - for summary statistics
        format: QUERY IP CPU START_DATA END_DATA
        example: QUERY 192.168.0.1 1 2014-10-31 00:00 2014-10-31 00:05
    """,
        'load': """
    LOAD - for average load statistics selected CPU
        format: LOAD IP CPU START_DATA END_DATA
        example: LOAD 192.168.0.1 2 2014-10-31 00:00 2014-10-31 00:05
    """,
        'stat': """
    STAT - for average load statistics all CPU-s
        format: STAT IP START_DATA END_DATA
        example: STAT 192.168.0.1 2014-10-31 00:00 2014-10-31 00:05
    """,
        'exit': """
    Exit - for exit
    """
    }
    if hp:
        return hptxt.get(hp)
    else:
        return ''.join(hptxt.values())


def stime(dlist):
    tdata = dlist[-4:]
    st = datetime.strptime("{} {}".format(tdata[0], tdata[1]), "%Y-%m-%d %H:%M")
    end = datetime.strptime("{} {}".format(tdata[2], tdata[3]), "%Y-%m-%d %H:%M")
    return "{} and {}".format(int(time.mktime(st.timetuple())), int(time.mktime(end.timetuple())))


def run_cmd(data):
    start = time.time()

    cc = data[0]
    query = "SELECT time,cpu,load FROM LOGS where ip='{0}' and time between {1};"
    query = query.format(data[1], stime(data))
    # cur = db.cursor()
    res = cursor.execute(query).fetchall()

    try:
        if cc == 'query' and len(data) == 7:
            for val in res:
                tm = datetime.fromtimestamp(int(val[0])).strftime('%Y-%m-%d %H:%M')
                if int(data[2]) == int(val[1]):
                    print('{} {} {}%'.format(tm, data[2], val[2]))

        elif cc == 'load' and len(data) == 7:
            cnt = 0
            load = 0
            for val in res:
                if int(data[2]) == int(val[1]):
                    cnt += 1
                    load = load + int(val[2])

            print('{}%'.format(load / cnt))

        elif cc == 'stat' and len(data) == 6:
            tmp = {}
            for val in res:
                if val[1] not in tmp:
                    tmp[val[1]] = [val[2]]

                tmp[val[1]].append(val[2])

            for key, val in tmp.iteritems():
                print('{}: {}%'.format(key, sum(val) / len(val)))
                

        else:
            print('Wrong command format for {}.'.format(cc))
            print(help(cc))
        # if query:
            #
            # print(res)
        
    except Exception as err:
        print(err)
    print('Execution time: {}'.format(time.time()-start))

def parser(text):
    cmd = text.lower()
    # update data
    if cmd.lower() == 'refresh':
        update_data()
    # Show help info
    elif cmd in ['help', '?', 'h']:
        print(help())
    # Exit
    elif cmd == 'exit':
        print('By-by!')
    # Run commands
    else:
        command = []
        for name in ['query', 'load', 'stat']:
            if cmd.find(name) > -1:
                command = cmd.split()
        if command:
            if len(command) == 1:
                print(help(command[0]))
            else:
                run_cmd(command)
        else:
            print('Wrong command. Please use "?" for show help.')


def input_loop():
    print('USE: h, Help - for HELP\nUse TAB for autocomplete.')
    update_data()
    line = ''
    while line.lower() != 'exit':
        line = raw_input('[?]Help:> ')
        parser(line)



#  Register our completer function
readline.set_completer(Completer(['Help', 'Exit', 'REFRESH', 'QUERY', 'LOAD', 'STAT']).complete)

# Use the tab key for completion
readline.parse_and_bind('tab: complete')


if __name__ == "__main__":
    if flist:
        input_loop()
