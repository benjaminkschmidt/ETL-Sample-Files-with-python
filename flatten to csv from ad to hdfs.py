import string
import json
import csv
from pprint import pprint

from hdfs import InsecureClient

import pandas as pd
username = 'bs87118g'

hdfs_client = InsecureClient('http://dlake02.creighton.edu:50070', user=username)
Load AD data into Python dict
Read in the file as a bytes object
Decode the bytes object to string. Filter out characters that aren’t printable (e.g. the null character \x00).
Load the string data into a dict.
with hdfs_client.read('/hdfslnd/active_directory/ad.min.json') as reader:
    raw_bytes = reader.read()

raw_str = raw_bytes.decode(errors='ignore')
clean_str = [c for c in raw_str if c in string.printable]

ad_json_str = ''.join(clean_str)
ad_json = json.loads(ad_json_str)
with open('ad_json.csv', 'wb') as csv_file:
    writer = csv.writer(csv_file)
    for key, value in ad.json.items():
       writer.writerow([key, value])
       # The line below will upload the csv file to hdfs, anticipate adjustments to be made during the hive table process
       # hdfs_client.write('/hdfslnd/active_directory', data='ad.csv', permission=None, blocksize=None, replication=None, buffersize=None, append=False, encoding=None )