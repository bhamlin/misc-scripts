#!/usr/bin/python3

XML_TEMPLATE = ("<?xml version='1.0'?>" +
"<outageSummary xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xmlns:xsd='http://www.w3.org/2001/XMLSchema' coopID='3'>" +
    "<totals>" +
        "<nbrOut>TOTAL_NUMBER_OF_METERS_OUT</nbrOut>" +
        "<nbrServed>TOTAL_NUMBER_OF_METERS</nbrServed>" +
    "</totals>" +
    "<regions type='County'>" +
        "<region>" +
            "<id>2409</id>" +
            "<name>Clarendon</name>" +
            "<nbrOut>TOTAL_NUMBER_OF_METERS_OUT_IN_COUNTY_2409</nbrOut>" +
            "<nbrServed>TOTAL_NUMBER_OF_METERS_IN_COUNTY_2409</nbrServed>" +
        "</region>" +
        "<region>" +
            "<id>2423</id>" +
            "<name>Kershaw</name>" +
            "<nbrOut>TOTAL_NUMBER_OF_METERS_OUT_IN_COUNTY_2423</nbrOut>" +
            "<nbrServed>TOTAL_NUMBER_OF_METERS_IN_COUNTY_2423</nbrServed>" +
        "</region>" +
        "<region>" +
            "<id>2426</id>" +
            "<name>Lee</name>" +
            "<nbrOut>TOTAL_NUMBER_OF_METERS_OUT_IN_COUNTY_2426</nbrOut>" +
            "<nbrServed>TOTAL_NUMBER_OF_METERS_IN_COUNTY_2426</nbrServed>" +
        "</region>" +
        "<region>" +
            "<id>2438</id>" +
            "<name>Sumter</name>" +
            "<nbrOut>TOTAL_NUMBER_OF_METERS_OUT_IN_COUNTY_2438</nbrOut>" +
            "<nbrServed>TOTAL_NUMBER_OF_METERS_IN_COUNTY_2438</nbrServed>" +
        "</region>" +
    "</regions>" +
"</outageSummary>")

CONFIG = '''
{
    "counties": [
        {"id": 2409, "name": "Clarendon"},
        {"id": 2423, "name": "Kershaw"},
        {"id": 2426, "name": "Lee"},
        {"id": 2438, "name": "Sumter"}
    ],
    "counts": [
        {"name": "Clarendon", "count": 4468},
        {"name": "Kershaw", "count": 5423},
        {"name": "Lee", "count": 2933},
        {"name": "Sumter", "count": 21280}
    ],
    "url": "http://10.50.10.181:7575/api/weboutageviewer/get_live_data"
}
'''

import json
import xmltodict

from functools import reduce
from gzip import GzipFile
from operator import add
from urllib.request import urlopen, Request

config = json.loads(CONFIG)

def get_live_data(url):
    with urlopen(url) as response:
        data = json.loads(response.read().decode())
    return data

def parse_raw_data(raw):
    data = dict()
    if 'Areas' in raw and raw['Areas']:
        for area in raw['Areas']:
            data[area['County']] = str(area['Count'])
    return data


total = dict()
for entry in config['counts']:
    total[entry['name']] = str(entry['count'])

cid = dict()
data = dict()
for county in config['counties']:
    id, name = county['id'], county['name']
    cid[name] = str(id)
    data[name] = '0'
data.update(parse_raw_data(get_live_data(config['url'])))

ct_out = str(reduce(add, map(int, [data['Clarendon'], data['Kershaw'], data['Lee'], data['Sumter']])))
ct = str(reduce(add, map(int, [total['Clarendon'], total['Kershaw'], total['Lee'], total['Sumter']])))

output = XML_TEMPLATE

output = output.replace('TOTAL_NUMBER_OF_METERS_OUT_IN_COUNTY_2409', data['Clarendon'])
output = output.replace('TOTAL_NUMBER_OF_METERS_IN_COUNTY_2409', total['Clarendon'])
output = output.replace('TOTAL_NUMBER_OF_METERS_OUT_IN_COUNTY_2423', data['Kershaw'])
output = output.replace('TOTAL_NUMBER_OF_METERS_IN_COUNTY_2423', total['Kershaw'])
output = output.replace('TOTAL_NUMBER_OF_METERS_OUT_IN_COUNTY_2426', data['Lee'])
output = output.replace('TOTAL_NUMBER_OF_METERS_IN_COUNTY_2426', total['Lee'])
output = output.replace('TOTAL_NUMBER_OF_METERS_OUT_IN_COUNTY_2438', data['Sumter'])
output = output.replace('TOTAL_NUMBER_OF_METERS_IN_COUNTY_2438', total['Sumter'])

output = output.replace('TOTAL_NUMBER_OF_METERS_OUT', ct_out)
output = output.replace('TOTAL_NUMBER_OF_METERS', ct)

# import xml.dom.minidom as mxml
# print(mxml.parseString(output).toprettyxml())
print(output, end='')
