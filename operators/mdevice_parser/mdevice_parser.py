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

import time
import json


def time_millis():
    return int(round(time.time() * 1000))


class MDevice:

    def __init__(
        self,
        id,
        company,
        person,
        index_date,
        device_index_date,
        status_date,
        status,
        product,
        ):
        self.id = id
        self.company = company
        self.person = person
        self.index_date = index_date
        self.device_index_date = device_index_date
        self.status_date = status_date
        self.status = status
        self.product = product

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=2)


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
    return MDevice(
        id=a[0],
        company=a[1],
        person=a[3],
        index_date=time_millis(),
        device_index_date=a[10],
        status_date=a[11],
        status=a[12],
        product=a[21],
        )


import csv
with open('pmn9195.txt', 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter='|')
    counter = 0
    try:
        for row in reader:
            counter = counter + 1
            if counter > 1:
                d = line_to_mdevice(row)
                if d != None:
                    print d.to_json()
    except Exception, e:
        print 'exception: ', e, counter
