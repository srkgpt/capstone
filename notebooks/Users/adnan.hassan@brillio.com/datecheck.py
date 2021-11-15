# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("storage","capstonebr")
dbutils.widgets.text("container","capstonedata")
dbutils.widgets.text("clientid","11fff788-5d3c-487c-a255-6db7f2f2cac3")
dbutils.widgets.text("secret","phk7Q~RAnMsSN-V96u4zLNjYSMn2z.N6zR.sY")
dbutils.widgets.text("tenantid","97984c2b-a229-4609-8185-ae84947bc3fc")
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



# COMMAND ----------



# COMMAND ----------

df.select(f.round("POPULATION")).show()

# COMMAND ----------

