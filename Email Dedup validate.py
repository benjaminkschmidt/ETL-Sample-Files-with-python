%spark.pyspark

from __future__ import division, print_function

import string
import json
from datetime import datetime
from pprint import pprint
from collections import defaultdict

import hdfs
import uuid

import pandas as pd
import numpy as np

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SQLContext
from pyspark.sql.types import *
from cassandra import cqlengine as ce


from email_validator import validate_email
import re

import matplotlib.pyplot as plt
import seaborn as sns
sns.set(style="whitegrid", color_codes=True)
%spark.pyspark

conf = SparkConf() \
        .setAppName('Zeppelin') \
        .set('spark.cassandra.connection.host','10.41.49.1') \
        .set('spark.master', 'yarn-client')

sc = SparkContext.getOrCreate(conf=conf)

hive_context = HiveContext(sc)
%spark.pyspark

email_query = " SELECT Email_ID" \
              "   , NetID" \
              "   , Email_Type" \
              "   , Preferred_Ind_From_Advance_Banner" \
              "   , Email_Address" \
              "   , Proxy_Email_From_AD" \
              "   , Data_Source" \
              " FROM data_lake.combined_email"
emails = hive_context.sql(email_query)
Push Out to Pandas
Take advange of Pandas’ apply function in conjuction with UDFs to clean and manipulate the data
email_pd = emails.toPandas()
Clean the Data
Write a customized set of logics to strip out all metacharacters from phone number with the exception of the plus sign +
Deal with each specific phone length case separately
% spark.pyspark


def email_cleaner(email):
    # makes each email lowercase and a string
    email = email.lower()
    # removes unwanted whitespace from emails (includes spaces, tabs, etc.)
    email = email.translate(string.whitespace)
    return email
%md
Validate Email
Write a custom function to determine whether each email is valid or not
%spark.pyspark

def email_validator(email):
    email_Valid_regex=r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
    if re.match(email_Valid_regex, email)==True:
            #v=validate_email(email)
            #email=v['email']
            return True
    else:
        return False
    % spark.pyspark

    def Proxy_cleaner(email):
        prelimterms = r"(^\SMTP:|^\smtp:)"
        proxy = re.sub(prelimterms, '', email)
        return proxy

    % spark.pyspark

    def Proxy_validate(Proxyemail):
        ProxyMatch = r"(^\([a-zA-Z]){3}([0-9]){5}"
        if re.match(ProxyMatch, Proxyemail) == True:
            return True
        else:
            return False

Apply the Functions
Use the apply function to build new columns
%spark.pyspark

email_pd['Clean_Email'] = email_pd['Email_Address'].apply(email_cleaner)
email_pd['Email_Valid'] = email_pd['Clean_Email'].apply(email_validator)
email_pd['Clean_ProxyEmail_From_AD']=email_pd['Proxy_Email_From_AD'].apply(Proxy_cleaner)
email_pd['NetidAsProxyEmail'] = email_pd['Clean_ProxyEmail_From_AD'].apply(Proxy_validate)
emailHive= hive_context.createDataFrame(email_pd)
emailHive.saveAsTable('data_lake.combined_email', mode='overwrite')