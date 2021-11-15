# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("storage","capstonebatch1")
dbutils.widgets.text("container","capstone")
dbutils.widgets.text("clientid","5dfd2400-29b4-40c0-8c9b-953dd22900d5")
#dbutils.widgets.text("secret","phk7Q~RAnMsSN-V96u4zLNjYSMn2z.N6zR.sY")
dbutils.widgets.text("tenantid","3882b70d-a91e-468c-9928-820358bfbd73")
storage = dbutils.widgets.get("storage")
print(storage)
container = dbutils.widgets.get("container")
print(container)
clientid = dbutils.widgets.get("clientid")
print(clientid)
#secret = dbutils.widgets.get("secret")
#print(secret)
tenantid = dbutils.widgets.get("tenantid")
print(tenantid)
secret = dbutils.secrets.get(scope="secret", key="secret")
#dbutils.widgets.removeAll()

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+storage+".dfs.core.windows.net", clientid)
spark.conf.set("fs.azure.account.oauth2.client.secret."+storage+".dfs.core.windows.net", secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+storage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")


# COMMAND ----------

 df = spark.read.format("csv").option("header","true").load("abfss://"+container+"@"+storage+".dfs.core.windows.net/covid19Vaccination.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as f
#df2 = df.withColumn("badRecord", f.when(f.to_date(f.col("DATE"),"dd-MM-yyyy HH:mm").isNotNull, False).otherwise(True))
df2=df.withColumn("badRecord",f.to_date(f.col("DATE"), "dd-MM-yyyy HH:mm"))
df2=df2.where(df2.badRecord.isNull())



# COMMAND ----------

df2.show()

# COMMAND ----------

#df3=df2.select(f.date_format(f.col("DATE"),"dd:MM:yyyy HH:mm").alias("new"))
#df3=df2.select(f.col("DATE"),f.when(f.to_date(f.col("DATE"),"dd-MM-yyyy HH:mm").isNull,f.date_format(f.col("DATE"),"dd-MM-yyyy HH:mm")).otherwise("unknown")).alias("new")


# COMMAND ----------

#df2.select(f.date_format('DATE','yyyy-MM-dd HH:mm').alias('new_dt')).show()

# COMMAND ----------

df3.show()

# COMMAND ----------


df2.select(f.date_format('DATE', 'MM/dd/yyy').alias('date')).collect()

# COMMAND ----------

df4 = spark.createDataFrame([('12-04-2021',)], ['a'])
df4.select(f.date_format('a', 'dd/MM/yyyy').alias('date')).collect()

# COMMAND ----------

output_format = 'dd/MM/yyyy'
df5=df4.select(f.date_format(
    f.unix_timestamp("a", "MM-dd-yyyy").cast("timestamp"), 
    output_format
))

# COMMAND ----------

df5.show()

# COMMAND ----------

df.select(f.round("POPULATION")).show()

# COMMAND ----------

dftemp = spark.read.format("csv").option("header","true").load("abfss://"+container+"@"+storage+".dfs.core.windows.net/covid19Vaccination.csv")

# COMMAND ----------

dftemp.show()

# COMMAND ----------

datelist=[x['DATE'] for x in dftemp.collect()]
datelist=set(datelist)
print(datelist)

# COMMAND ----------

from datetime import datetime
from dateutil import parser
for i in datelist:
    try:
        j=parser.parse(i)
    except:
        j=datetime.strptime(i,'%Y-%d-%m %H:%M')
    #j=datetime.strptime(i,'%Y-%m-%d')
    j=datetime.strftime(j,'%Y-%m-%d %H:%M')
    print(j)
    dftemp=dftemp.replace(i,j)

# COMMAND ----------

dftemp.show()

# COMMAND ----------

