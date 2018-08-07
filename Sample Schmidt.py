%spark.pyspark

from __future__ import division, print_function

import re

import numpy as npdata
import pandas as pd
import pyspark.sql.functions as sf
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from cassandra import cqlengine as ce

% spark.pyspark

quote_re = re.compile(r"\"")


def clean_quotes(in_value):
    """Helper function for cleaning quotes and default spaces from Advance data.
    """
    _out_value = in_value

    try:
        _out_value = quote_re.sub('', in_value)  # .strip()
    except TypeError:
        pass

    return _out_value


def safe_concat(df, column, postfix):
    _col = sf.when(df[column] != ' ', sf.concat(df[column], sf.lit(postfix))).otherwise('').alias(column)
    return _col
#Spark setup
#Set up the Spark connection and Hive context.
conf = SparkConf() \
        .setAppName('Zeppelin') \
        .set('spark.cassandra.connection.host','10.41.49.1') \
        .set('spark.master', 'yarn-client')

sc = SparkContext.getOrCreate(conf=conf)

hive_context = HiveContext(sc)
#Define Hive queries
#By pushing a query down to Hive instead of loading the whole table into a DataFrame, we minimize the amount of bandwidth and memory that Spark needs to use.
ad_phone_query = " SELECT SAMAccountName" \
                "   , MobilePhone" \
                "   , telephoneNumber" \
                " FROM data_lake.ad_data_raw"

advance_phone_query = " SELECT lower(NetID) AS NetID" \
                     "   , ID_Number" \
                     "   , xsequence" \
                     "   , telephone_type_Code" \
                     "   , telephone_number" \
                     "   , extension" \
                     "   , country_code" \
                     "   , area_code" \
                     " FROM data_lake.advance_telephone_raw" \
                     " WHERE telephone_status_code = '\"A\"'"



hub_phone_query = " SELECT lower(NetID) AS NetID" \
                  "   , Person_Key_Value" \
                  "   , Phone_Type_Desc" \
                  "   , Country_Code" \
                  "   , National_Destination_Code" \
                  "   , Subscriber_Number" \
                  " FROM data_lake.hub_phone_raw" \
                  " WHERE netid != 'NETID'"


banner_phone_query = " SELECT lower(NetID) AS NetID" \
                     "   , PIDM" \
                     "   , Sequence_Number" \
                     "   , Telephone_Description" \
                     "   , Telephone_Area_Code" \
                     "   , Telephone_Number" \
                     " FROM data_lake.banner_phones_plus_netid"
ad_phone = hive_context.sql(ad_phone_query)

advance_phone = hive_context.sql(advance_phone_query)

hub_phone = hive_context.sql(hub_phone_query)

banner_phone = hive_context.sql(banner_phone_query)


#Clean up Advance data
#The data coming in from Advance has double quotes around each field. We need to clean them up. We use Pandas for this part because it has a handy applymap method that works elementwise.

advance_phone_pd = advance_phone.toPandas()
advance_phone_pd_clean = advance_phone_pd.applymap(lambda x: clean_quotes(x))
adv_phone_clean = hive_context.createDataFrame(advance_phone_pd_clean)
adv_phone_clean = hive_context.createDataFrame(advance_phone_pd_clean)
banner_full_phone = (banner_phone

    .withColumn('Full_Phone',
                sf.rtrim(
                    sf.concat(
                        safe_concat(banner_phone, 'Telephone_Area_Code', ''),
                        safe_concat(banner_phone, 'Telephone_Number', ''), )))

    .withColumn('Data_Source', sf.lit('Banner'))

    .select(
    sf.col('NetID'),
    sf.col('PIDM').alias('Source_ID'),
    sf.col('Telephone_Description').alias('Phone_Type'),
    sf.col('Full_Phone'),
    sf.col('Data_Source')))
hub_full_phone = (hub_phone
    .withColumn('Full_Phone',
                sf.rtrim(
                    sf.concat(
                        safe_concat(hub_phone, 'Country_Code', ''),
                        safe_concat(hub_phone, 'National_Destination_Code', ''),
                        safe_concat(hub_phone, 'Subscriber_Number', ''))))

    .withColumn('Data_Source', sf.lit('Hub'))

    .select(
    sf.col('NetID'),
    sf.col('Person_Key_Value').alias('Source_ID'),
    sf.col('Phone_Type_Desc').alias('Phone_Type'),
    sf.col('Full_Phone'),
    sf.col('Data_Source')))
advance_full_phone = (adv_phone_clean
    .withColumn('Full_Phone',
                sf.rtrim(
                    sf.concat(
                        safe_concat(adv_phone_clean, 'country_code', ''),
                        safe_concat(adv_phone_clean, 'area_code', ''),
                        safe_concat(adv_phone_clean, 'telephone_number', ''))))
    .withColumn('Data_Source',
                sf.lit('Advance'))

    .withColumn('Source_ID',
                sf.concat_ws('-', 'ID_Number', 'xsequence'))
    .select(
        sf.col('NetID'),
        sf.col('Source_ID'),
        sf.col('telephone_type_code').alias('Phone_Type'),
        sf.col('Full_Phone'),
        sf.col('Data_Source')))
combined_phone = (advance_full_phone
    .unionAll(ad_full_phone)
    .unionAll(hub_full_phone)
    .unionAll(banner_full_phone)
    .withColumn('Phone_ID',
                sf.monotonically_increasing_id())
    .select(
        'Phone_ID',
        'NetID',
        'Source_ID',
        'Phone_Type',
        'Full_Phone',
        'Data_Source'))
#Clean the Phone Numbers
#We want to strip off all () and - then create a new column with the digit count called Phone_Length
combined_pd = combined_phone.toPandas()
#Create Functions to Clean and Count the Phone Numbers
def phone_cleaner(phone_number):
    return re.sub('[^A-Za-z0-9]+', '', phone_number)


def phone_counter(phone_number):
    return len(phone_number)
phone_pd['Clean_Phone'] = phone_pd['Full_Phone'].apply(phone_cleaner)
phone_pd['Phone_Length'] = phone_pd['Clean_Phone'].apply(phone_counter)
combined_phone_plus_count = hive_context.createDataFrame(phone_pd)
#Save phone data to Hive
combined_phone_plus_count.saveAsTable('data_lake.combined_phone', mode='overwrite')
ad_phone_query = " SELECT SAMAccountName" \
                 "   , TelephoneNumber" \
                 "   , HomePhone" \
                 "   , MobilePhone" \
                 "   , Mobile" \
                 "   , OfficePhone" \
                 "   , OtherTelephone" \
                 "   , Pager" \
                 "   , FacsimileTelephoneNumber" \
                 "   , OtherFacsimileTelephoneNumber" \
                 " FROM data_lake.ad_data_raw"
ad_phone = hive_context.sql(ad_phone_query)