
import re
import string
from email_validator import validate_email


#Has 2 invalid emails
Test=["dapibus@Lorem.net", "Proin.velit@nequenon.edu", "varius@Cras.edu", "tempus.scelerisque@Vivamus.edu", "erat.in.consectetuer@molestiepharetranibh.edu", "id@semelit.co.uk", "blandit@euismodenim.org", "Sed.nulla.ante@et.net", "bkschmidt", "1245", "ILEK@gmail.com"]
valid1=[]
valid2=[]
email_Valid_regex=r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
ProxyMatch=r"(^\[a-zA-Z]{3}d{3}+@$)"
ProxyMatchGuest=r"(^\[a-zA-Z]{3}d{3}[a-z]{1}+@$)"
def validEmails(emails):
    print(len(emails))
    for email in emails:
        email=str(email).lower()
        email.translate(None, string.whitespace)
        if bool(re.match(email_Valid_regex, email))==True:
            valid1.append(email)
    print(len(valid1))
    for email in valid1:
            if bool(validate_email(email))==True: # validate and get info
                valid2.append(email)
    print(len(valid2))
validEmails(Test)
%spark.pyspark

conf = SparkConf() \
        .setAppName('Zeppelin') \
        .set('spark.cassandra.connection.host','10.41.49.1') \
        .set('spark.master', 'yarn-client')

sc = SparkContext.getOrCreate(conf=conf)

hive_context = HiveContext(sc)

email_query = " SELECT Phone_ID" \
              "   , NetID" \
              "   , Email_ID" \
              "   , Email_Type" \
              "   , Preferred_Ind" \
              "   , Email_Address" \
              "   , Proxy_Email"
              "   , Data_Source" \
              " FROM data_lake.combined_email"

email = hive_context.sql(email_query)
email.show()
for email in email.Email_Address:
    email = str(email).lower()
    email.translate(None, string.whitespace)
    if bool(re.match(email_Valid_regex, email)) == True:
        if bool(validate_email(email)) == True:  # validate and get info
            REPLACE
            WITH
            BOOLEAN
            COLUMN!
            valid2.append(TRUE)
        else:
            valid2.append(FALSE)
    else:
        valid2.append(FALSE)

    for proxy in email.Proxy_Address:
        if bool(re.match(ProxyMatch | ProxyMatchGuest, email))=True:
            NetIDProxy.append(True)
        else:
            NetIDProxy.append(False)
