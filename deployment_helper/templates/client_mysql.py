#!/usr/bin/env python

import mysql.connector
import os
import subprocess
import socket
import sys
import time
import random
import argparse

def get_hostname():
    FNULL = open(os.devnull, 'w')
    url = 'http://169.254.169.254/latest/meta-data/public-hostname'
    try:
     return subprocess.check_output(['curl', '--connect-timeout', '1', url], stderr=FNULL)
    except subprocess.CalledProcessError:
        return socket.gethostname()

config = {
    'charset': 'utf8',
    'user': 'mysql_user',
    'password': 'mysql_password',
    'port': '15306',
    'database': 'messagedb',
    'connection_timeout': 15,
    'raise_on_warnings': False,
    }

mysql_config = {
    'port': '3306',
    }

vtgate_config = {
    'port': '15306',
}

cnx = None
cursor = None
args = None

def parse_args():
    global args
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', dest='server', default='vtgate')
    parser.add_argument('--host', dest='host', default=get_hostname())
    parser.add_argument('--timeout', dest='timeout', type=float, default='5.0')
    parser.add_argument('--qps', dest='qps', type=float, default='10.0')
    parser.add_argument('--read-write-ratio', dest='read_write_ratio', type=float, default='0.8')
    args = parser.parse_args()

def connect():
    global cnx, cursor
    config.update(dict(connection_timeout=args.timeout))
    config.update(dict(host=args.host))
    if args.server == 'vtgate':
        config.update(vtgate_config)
    else:
        config.update(mysql_config)
    try:
        print '*%s* @ %s:%s' % (args.server, config['host'], config['port'])
        cnx = mysql.connector.connect(**config)
    except Exception as e:
        print e
        sys.exit()
    else:
        cursor = cnx.cursor(buffered=True)

time_created_values = []
write_counter = 0
read_counter = 0
error_counter = 0

def throttled(f):
    def wrapper():
        start_time = time.time()
        f()
        under_budget = (1 / args.qps) - (time.time() - start_time)
        if under_budget > 0:
            time.sleep(under_budget)
    return wrapper

@throttled
def write_row():
    global write_counter
    global error_counter
    global cnx, cursor
    insert_sql = 'INSERT INTO messages (page, time_created_ns, message) VALUES (%s, %s, %s)'
    page = random.randint(1, 100)
    time_created_ns = int(time.time() * 1e9)
    time_created_values.append(time_created_ns)
    message = 'V is for speed'
    try:
        cursor.execute(insert_sql, (page, time_created_ns, message))
        cnx.commit()
        write_counter += 1
    except Exception as e:
        error_counter += 1
        print e
        connect()
        #cnx.rollback()

def exec_read_query(argc, **argv):
    global read_counter
    global error_counter
    try:
        cursor.execute(argc, argv)
        read_counter += 1
        return True
    except Exception as e:
        error_counter += 1
        print e
        connect()
        return False

@throttled
def read_row():
    global read_counter
    global error_counter
    query_sql = 'select * from messages where time_created_ns = "%s"'
    time_created_ns = random.choice(time_created_values)
    try:
        cursor.execute(query_sql, (time_created_ns))
        read_counter += 1
    except Exception as e:
        error_counter += 1


def read_row_count():
    query_sql = 'select count(*) from messages'
    if (exec_read_query(query_sql)):
        for (count) in cursor:
            return count[0] or 0
    else:
        return 0

def log(total_time, row_count):
    if row_count is None:
        row_count = 0
    read_qps = read_counter / total_time
    write_qps = write_counter / total_time
    msg = '\relapsed=%4d rows(count=%4d) read(count=%4d qps=%.2f) write(count=%4d qps=%.2f) error(count=%4d)' % (int(total_time), row_count, read_counter, read_qps, write_counter, write_qps, error_counter)
    sys.stdout.write(msg)
    sys.stdout.flush()

def run():
    total_time = 0
    while True:
        start_time = time.time()
        read_queries = int(args.qps * args.read_write_ratio)
        write_queries = int(args.qps - read_queries)
        for i in xrange(write_queries):
            write_row()
        for i in xrange(read_queries):
            read_row()
        time_elapsed = time.time() - start_time
        total_time += time_elapsed
        log(total_time, read_row_count())

if __name__ == '__main__':
    parse_args()
    connect()
    run()
