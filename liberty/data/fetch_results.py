#!/usr/bin/env python2
"""
fetch_results.py
Python script that fetches the results of mesos executors

Example:

python fetch_results.py -m 23.251.149.196:5050 --regex ".*throughput.txt" \
       -e 15241255235284647057-kafka_source 9415723926703574995-kafka_source

Omitting -e fetches all current tasks
python fetch_results.py -m 23.251.149.196:5050 --regex ".*throughput.txt"

"""

import os
import sys
import argparse
import httplib
import json
import shutil
import re

# Use PERMIT_FILES unless user supplied regex was given
# Group output by node ip

DATA_DIR = './mesos-run-data'
PERMIT_FILES = [ 'incoming_throughput', 'outgoing_throughput',
                 'hardware_usage_monitor' 'principal_latencies',
                 'dispatcher_latencies', 'stderr', 'stdout', '.INFO' ]

def whitelist_file(filename, regex):
    if regex == None:
        filename = filename.split('/')[-1]
        return any(map(lambda x: x in filename , PERMIT_FILES))
    # Otherwise if regex.match(filename)
    x = re.compile(regex)
    return x.match(filename) != None

def generate_options():
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--master", metavar="mesos-master", action="store",
                        help="i.e: 123.456.789.95", required=True)
    parser.add_argument('-e','--executor_ids', nargs='+',
                        help='11260583907712781801-unique, ...')
    parser.add_argument('-r', '--regex', action="store", help='*.txt')
    return parser

def mesos_http_req(addr, endpoint, isJson=True):
    def printText(txt):
        lines = txt.split('\n')
        for line in lines:
            return line.strip()
    client = httplib.HTTPConnection(addr)
    client.connect()
    client.request("GET", endpoint)
    response = client.getresponse()
    if response.status != httplib.OK:
        return None
    if isJson == True:
        info = printText(response.read())
        client.close()
        return json.loads(info, encoding="ISO-8859-1")
    return response.read()

def get_master_tasks(master_addr):
    return mesos_http_req(master_addr, "/tasks")

def get_slave_info(master_addr):
    return mesos_http_req(master_addr, "/slaves")

def get_slave_data(slave_addr):
    return mesos_http_req(slave_addr, "/files/debug")

def get_slave_browse(slave_addr, path):
    return mesos_http_req(slave_addr, "/files/browse?path=" + path)

def get_slave_file(slave_addr, path):
    return mesos_http_req(slave_addr, "/files/download?path=" + path, False)

def get_file_paths(sandbox, executor_ids):
    res = []
    for eid in executor_ids:
        plain_id = eid.split('-')[0]
        for path in sandbox.keys():
            if plain_id in path:
                res.append((eid, path))
    return res

def expand(addr, path, regex):
    res = []
    contents = get_slave_browse(addr, path)
    for content in contents:
        cpath = content['path']
        if content['size'] == 0:
            continue
        elif content['mode'][0] == 'd':
            res = res + expand(addr, cpath, regex)
        elif whitelist_file(cpath, regex):
            res.append(cpath)
    return res

def locate_data(slaves, executor_ids, regex=None):
    data = {}
    slave_info = slaves['slaves']
    for slave in slave_info:
        hostname = slave['hostname'] + ":5051"
        slave_id = slave['id']
        sandbox = get_slave_data(hostname)
        paths = get_file_paths(sandbox, executor_ids)
        if len(paths) > 0:
            data[hostname] = map(lambda x: (x[0], expand(hostname, x[1], regex))
                                 , paths)
    return data

def download_data(data):
    for slave_addr, runs in data.iteritems():
        if not os.path.exists(slave_addr) and len(runs) > 0:
            os.makedirs(slave_addr)
        for run in runs:
            executor_id, data_paths = run
            print "Executor id: ", executor_id
            executor_path = slave_addr + '/' + executor_id
            if not os.path.exists(executor_path):
                os.makedirs(executor_path)
            for mesosfile in data_paths:
                filename = executor_path + '/' + mesosfile.split('/')[-1]
                print "... => Downloading %s from slave %s" % (filename, slave_addr)
                with open(filename, 'a') as fileh:
                    fileh.write(get_slave_file(slave_addr, mesosfile))

def fetch_current_tasks(master_addr):
    tasks = get_master_tasks(master_addr)['tasks']
    running = filter(lambda x: x['state'] == 'TASK_RUNNING', tasks)
    return map(lambda x: x['id'], running)

def main():
    parser = generate_options()
    options = parser.parse_args()
    executor_ids = fetch_current_tasks(options.master) if options.executor_ids is None else options.executor_ids
    locations = locate_data(get_slave_info(options.master),
                            executor_ids, options.regex)
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.makedirs(DATA_DIR)
    os.chdir(DATA_DIR)
    download_data(locations)

if __name__ == "__main__":
    main()
