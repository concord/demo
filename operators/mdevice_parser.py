#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
KNUMBER -> id
APPLICANT -> company
CONTACT -> person
DATERECEIVED -> device_index_date
DECISIONDATE -> status_date
DECISION -> status
DEVICENAME -> product
"""
import os
import time
import json
import csv
import requests
from zipfile import ZipFile
import concord
from concord.computation import (
    Computation,
    Metadata,
    serve_computation
)

def time_millis():
    return int(round(time.time() * 1000))


class MDevice:
    def __init__(self, id, company, person, index_date,
                 device_index_date, status_date, status, product,):
        self.id = id
        self.company = company
        self.person = person
        self.index_date = index_date
        self.device_index_date = device_index_date
        self.status_date = status_date
        self.status = status
        self.product = product
    def to_json(self):
        try:
            return json.dumps(self, default=lambda o: o.__dict__, indent=2)
        except:
            return None


# ['KNUMBER',       == 0
#  'APPLICANT',     == 1
#  'CONTACT',       == 2
#  'STREET1',       == 3
#  'STREET2',       == 4
#  'CITY',          == 5
#  'STATE',         == 6
#  'COUNTRY_CODE',  == 7
#  'ZIP',           == 8
#  'POSTAL_CODE',   == 9
#  'DATERECEIVED',  == 10
#  'DECISIONDATE',  == 11
#  'DECISION',      == 12
#  'REVIEWADVISECOMM', 13
#  'PRODUCTCODE',   == 14
#  'STATEORSUMM',   == 15
#  'CLASSADVISECOMM',= 16
#  'SSPINDICATOR',  == 17
#  'TYPE',          == 18
#  'THIRDPARTY',    == 19
#  'EXPEDITEDREVIEW',= 20
#  'DEVICENAME']    == 21
#
def line_to_mdevice(line_arr):
    if len(line_arr) != 22:
        return None
    a = line_arr  # alias
    return MDevice(id=a[0], company=a[1], person=a[2], index_date=time_millis(),
                   device_index_date=a[10], status_date=a[11], status=a[12],
                   product=a[21])

# returns a tuple of name.zip and name.txt from the url
def download_zip_url(url):
    zip_name = os.path.basename(url)
    if zip_name == None or len(zip_name) <=0:
        return (None,None)
    with open(zip_name, 'wb') as handle:
        req = requests.get(url)
        if not req.ok:
            print "Something went wrong downloading the zip archive ", zip_name
            return (None,None)
        else:
            for block in req.iter_content(1024):
                handle.write(block)
    def zip_name_to_txt_name(n):
        name_parts = n.split(".")
        name_parts = name_parts[:len(name_parts)-1] # minus last elem
        name_parts.append("txt")
        return ".".join(name_parts)

    return (zip_name, zip_name_to_txt_name(zip_name))

class MedicalDeviceIterator:
    def __init__(self, url):
        self.url = url
        (zip_name, text_name) = download_zip_url(self.url)
        if zip_name == None or text_name == None:
            raise Exception("Errors downloading url")
        self.zip_name = zip_name
        self.text_name = text_name
        self.zip_handle = ZipFile(self.zip_name, 'r')
        self.handle = self.zip_handle.open(name=self.text_name, mode='r')
        self.reader = csv.reader(self.handle, delimiter='|')
        next(self.reader) # skip the header
        self.finished_parsing = False
        self.bad_records_parsed = 0
        self.records_parsed = 0

    def __iter__(self):
        return self

    def lines_read(self):
        return self.records_parsed

    # Returns a MDevice obj, and skips over bad records
    def next(self):
        if self.finished_parsing == True:
            raise StopIteration
        while True:
            try:
                line = next(self.reader)
                self.records_parsed = self.records_parsed + 1
                return line_to_mdevice(line)
            except StopIteration:
                print "Records parsed: ", self.records_parsed
                self.finished_parsing = True
                try:
                    self.handle.close()
                    self.zip_handle.close()
                except Exception as e:
                    print "Exception closing readers ", e
                raise StopIteration
            except Exception as e:
                self.bad_records_parsed = self.bad_records_parsed + 1
                print "Unhandled error in url parsing, skipping record: ", e

# for mdevice_obj in MedicalDeviceIterator("http://www.accessdata.fda.gov/premarket/ftparea/pmn96cur.zip"):
#     print mdevice_obj.to_json()

class MedicalDevicesParser(Computation):
    def init(self, ctx): pass
    def destroy(self): pass
    def process_timer(self, ctx, key, time): pass
    def process_record(self, ctx, record):
        for obj in MedicalDeviceIterator(str(record.value)):
            ctx.produce_record("m-device-urls", obj.id, obj.to_json())
    def metadata(self):
        return Metadata(
            name='m-device-parser',
            istreams=['m-device-urls'],
            ostreams=['m-devices-json'])

serve_computation(MedicalDeviceParser())
