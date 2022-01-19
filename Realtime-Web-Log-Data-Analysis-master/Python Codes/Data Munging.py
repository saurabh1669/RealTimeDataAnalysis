# -*- coding: utf-8 -*-
"""
Created on Sat Dec  7 19:10:34 2019

@author: Pushkar Vengurlekar
"""

from sqlalchemy import create_engine
import boto3
import psycopg2 as pg
import numpy as np
import pandas as pd
import geoip2.database
from botocore.exceptions import NoCredentialsError
from copy import deepcopy
reader = geoip2.database.Reader(r'C:\Users\pmven\Google Drive\1. myDocs\MSBA\MSBA 6330 Big Data\Trends\GeoLite2-City.mmdb')


log = pd.read_csv(r'C:\spark\vmfiles\IPNB\Trends\access.log', sep = '\n', nrows = 100000, names = ['string'])

host_pattern = r'(^\S+\.[\S+\.]+\S+)\s\S\s\S\s\[.*\]\s\".*\"\s\d*\s\d*\s\".*\"\s\".*\"\s\".*\"'
gap1_pattern = r'^\S+\.[\S+\.]+\S+\s(\S)\s\S\s\[.*\]\s\".*\"\s\d*\s\d*\s\".*\"\s\".*\"\s\".*\"'
gap2_pattern = r'^\S+\.[\S+\.]+\S+\s\S\s(\S)\s\[.*\]\s\".*\"\s\d*\s\d*\s\".*\"\s\".*\"\s\".*\"'
ts_pattern =   r'^\S+\.[\S+\.]+\S+\s\S\s\S\s\[(.*)\]\s\".*\"\s\d*\s\d*\s\".*\"\s\".*\"\s\".*\"'
status_pattern = r'^\S+\.[\S+\.]+\S+\s\S\s\S\s\[.*\]\s\".*\"\s(\d*)\s\d*\s\".*\"\s\".*\"\s\".*\"'
status2_pattern = r'^\S+\.[\S+\.]+\S+\s\S\s\S\s\[.*\]\s\".*\"\s\d*\s(\d*)\s\".*\"\s\".*\"\s\".*\"'
get_pattern = r'^\S+\.[\S+\.]+\S+\s\S\s\S\s\[.*\]\s\"(.*)\"\s\d{3}\s\d*\s\".*\"\s\".*\"\s\".*\"'
url_pattern = r'^\S+\.[\S+\.]+\S+\s\S\s\S\s\[.*\]\s\".*\"\s\d*\s\d*\s\"(.*)\"\s\".*\"\s\".*\"'
model_pattern = r'^\S+\.[\S+\.]+\S+\s\S\s\S\s\[.*\]\s\".*\"\s\d*\s\d*\s\".*\"\s\"(.*)\"\s\".*\"'
gap3_pattern = r'^\S+\.[\S+\.]+\S+\s\S\s\S\s\[.*\]\s\".*\"\s\d*\s\d*\s\".*\"\s\".*\"\s\"(.*)\"'

log['host'] = log['string'].str.extract(host_pattern, expand = True)
log['gap1'] = log['string'].str.extract(gap1_pattern, expand = True)
log['gap2'] = log['string'].str.extract(gap2_pattern, expand = True)
log['ts'] = log['string'].str.extract(ts_pattern, expand = True)
log['get'] = log['string'].str.extract(get_pattern, expand = True)
log['status'] = log['string'].str.extract(status_pattern, expand = True)
log['status2'] = log['string'].str.extract(status2_pattern, expand = True)
log['url'] = log['string'].str.extract(url_pattern, expand = True)
log['model'] = log['string'].str.extract(model_pattern, expand = True)
log['gap3'] = log['string'].str.extract(gap3_pattern, expand = True)
log = log.drop('string', axis = 1)

log2 = deepcopy(log)

lat = []
long = []
for i in range(len(log2['host'])):
    if log2['host'][i] != 'host':
        try:
            response = reader.city(log2['host'][i])
            lat.append(response.location.latitude)
            long.append(response.location.longitude)
        except:
            lat.append(0)
            long.append(0)

def trim(x):
    return x[:500]

log2['lat'] = lat
log2['long'] = long

log2 = log2[['host',  'model', 'status', 'status2', 'ts', 'url', 'lat', 'long', 'get']]
log2.columns = ['host', 'model', 'status', 'status2', 'time1', 'url', 'lat', 'log', 'get_method']

log2['get_method'] = log2['get_method'].apply(trim)

log2.to_csv(r'C:\Users\pmven\Google Drive\1. myDocs\MSBA\MSBA 6330 Big Data\Trends\s3\log_sample.csv', index = False)
log2 = pd.read_csv(r'C:\Users\pmven\Google Drive\1. myDocs\MSBA\MSBA 6330 Big Data\Trends\s3\log_sample.csv', parse_dates = [4])