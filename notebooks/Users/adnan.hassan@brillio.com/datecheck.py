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
