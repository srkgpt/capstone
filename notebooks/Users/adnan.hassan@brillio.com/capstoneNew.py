# Databricks notebook source
import pyspark.sql.functions as f
from functools import reduce
from datetime import date
from pyspark.sql.functions import lit,unix_timestamp
import time
import datetime
from pyspark.sql.types import StructType,StructField, StringType, FloatType



# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storage","storage name")
dbutils.widgets.text("container","container name")
dbutils.widgets.text("clientid","client_id")
dbutils.widgets.text("secret","secret key")
dbutils.widgets.text("tenantid","tenant_id")
storage = dbutils.widgets.get("storage")
print(storage)
container = dbutils.widgets.get("container")
print(container)
clientid = dbutils.widgets.get("clientid")
print(clientid)
secret = dbutils.widgets.get("secret")
print(secret)
tenantid = dbutils.widgets.get("tenantid")
print(tenantid)
#dbutils.widgets.removeAll()

# COMMAND ----------



# COMMAND ----------

