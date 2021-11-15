# Databricks notebook source
pip install azure.storage.blob

# COMMAND ----------

pip install python-Levenshtein


# COMMAND ----------

pip install fuzzywuzzy

# COMMAND ----------

pip install country_list

# COMMAND ----------

# DBTITLE 1,Import modules
import pyspark.sql.functions as f
from functools import reduce
from datetime import date
from pyspark.sql.functions import lit,unix_timestamp
import pyspark.sql
import time
import datetime
from pyspark.sql.types import StructType,StructField, StringType, FloatType, IntegerType
import pandas as pd
import matplotlib.pyplot as plt


# COMMAND ----------

# DBTITLE 1,Widget Creation
dbutils.widgets.removeAll()
dbutils.widgets.text("storage","capstonebatch1")
dbutils.widgets.text("container","capstone")
dbutils.widgets.text("clientid","5dfd2400-29b4-40c0-8c9b-953dd22900d5")
#dbutils.widgets.text("secret","wCJ7Q~jIuHjYhLC27F4zQftjQWXX0fNQ0TXTM")
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
#dbutils.widgets.removeAll()
secret = dbutils.secrets.get(scope="secret", key="secret")

# COMMAND ----------

# DBTITLE 1,ErrorLog Function
def ErrorLog(errorMsg):
    dict = [{'Error Message': errorMsg}]
    dfEMessage = spark.createDataFrame(dict)
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') 
    dfNewErrorLog = dfEMessage.withColumn('time',unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    dfErrorLog=spark.read.format("csv").option("header","True").load("abfss://"+container+"@"+storage+".dfs.core.windows.net/errorLog")
    dfErrorLog.show()
    dfErrorLog=dfErrorLog.union(dfNewErrorLog)
    dfErrorLog.show()
    dfErrorLog.coalesce(1).write.option("Header","True").save(path="abfss://"+container+"@"+storage+".dfs.core.windows.net/errorLog",
                                         format="csv",mode="overwrite") 
  

# COMMAND ----------

# DBTITLE 1,Extract Data from Storage Account

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+storage+".dfs.core.windows.net", clientid)
spark.conf.set("fs.azure.account.oauth2.client.secret."+storage+".dfs.core.windows.net", secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+storage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")


# COMMAND ----------

# DBTITLE 1,Load Athletes
try:
    dfAthletes = spark.read.format("csv").option("header","true").load("abfss://"+container+"@"+storage+".dfs.core.windows.net/Athletes.csv")
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

dfAthletes.show()

# COMMAND ----------

# DBTITLE 1,Load Medals
try:
    dfMedals = spark.read.format("csv").option("header","true").load("abfss://"+container+"@"+storage+".dfs.core.windows.net/Medals.csv")
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

dfMedals.show()

# COMMAND ----------

# DBTITLE 1,Load Teams
try:
    dfTeams = spark.read.format("csv").option("header","true").load("abfss://"+container+"@"+storage+".dfs.core.windows.net/Teams.csv") 
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

dfTeams.show()

# COMMAND ----------

# DBTITLE 1,Remove Columns in Medals without headers
try:
    for x in dfMedals.columns:
        if "_c" in x:
            dfMedals=dfMedals.drop(x)
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)
        
#fixneeded

# COMMAND ----------

dfMedals.show()

# COMMAND ----------

# DBTITLE 1,Get rows from Medals which have atleast one null value in it
try:
    dfnullmedal=dfMedals.where(reduce(lambda x, y: x | y, (f.col(x).isNull() | (f.col(x)=="null") for x in dfMedals.columns)))
    # Reduce-> implementing function across a sequence and in this case funct would be returning true if any of the value in the sequence is true which means atleast one of the column value is null
    dfnullmedal.show()
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

# DBTITLE 1,Getting blank rows from Medals and replacing with NA
try:
    dfMedalsblank=dfMedals.where(reduce(lambda x, y: x | y, (f.col(x).isNull() for x in dfMedals.columns)))
    dfMedalsblank.na.fill(value="NA").show()
    
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

# DBTITLE 1,getting rows from Medals with string null
try:
    dfnullMedals=dfMedals.where(reduce(lambda x, y: x | y, (f.col(x)=="null" for x in dfMedals.columns)))
    dfnullMedals.show()
    
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

# DBTITLE 1,Get rows from Athletes which have atleast one null value in it
#dfnullathlete=dfAthletes.where(reduce(lambda x, y: x | y, (f.col(x).isNull() | (f.col(x)=="null") for x in dfAthletes.columns)))
# Reduce-> implementing function across a sequence and in this case funct would be returning true if any of the value in the sequence is true which means atleast one of the column value is null
#dfnullathlete.show()

# COMMAND ----------

# DBTITLE 1,Getting blank rows from the Athletes and replacing with NA
try:
    dfAthletesblank=dfAthletes.where(reduce(lambda x, y: x | y, (f.col(x).isNull() for x in dfAthletes.columns)))
    dfAthletesblank.na.fill(value="NA").show()
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

# DBTITLE 1,getting rows from Athletes with string null 
try:
    dfnullAthletes=dfAthletes.where(reduce(lambda x, y: x | y, (f.col(x)=="null" for x in dfAthletes.columns)))
    dfnullAthletes.show()
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

# DBTITLE 1,Remove Columns in Teams without headers
try:
    for x in dfTeams.columns:
        if "_c" in x:
            dfTeams=dfTeams.drop(x)        
    dfTeams.show()
    
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

# DBTITLE 1,Get rows from Teams which have atleast one null value in it
dfnullTeams=dfTeams.where(reduce(lambda x, y: x | y, ((f.col(x)=="null") for x in dfTeams.columns)))
# Reduce-> implementing function across a sequence and in this case funct would be returning true if any of the value in the sequence is true which means atleast one of the column value is null
dfnullTeams.show()

# COMMAND ----------

dfTeamsblank=dfTeams.where(reduce(lambda x, y: x | y, (f.col(x).isNull() for x in dfTeams.columns)))

dfTeamsblank.na.fill(value="NA").show()


# COMMAND ----------

print("The percentage of null in Athlete is {}%".format((dfnullAthletes.count()/dfAthletes.count())*100))
print("The percentage of null in Medals is {}%".format((dfnullMedals.count()/dfMedals.count())*100))
print("The percentage of null in Teams is {}%".format((dfnullTeams.count()/dfTeams.count())*100))
print("The percentage of blank in Athlete is {}%".format((dfAthletesblank.count()/dfAthletes.count())*100))
print("The percentage of blank in Medals is {}%".format((dfMedalsblank.count()/dfMedals.count())*100))
print("The percentage of blank in Teams is {}%".format((dfTeamsblank.count()/dfTeams.count())*100))

# COMMAND ----------

from fuzzywuzzy import fuzz, process
fuzz.ratio("spaiN","Spain")

# COMMAND ----------

#from country_list import countries_for_language
#countries=countries_for_language('en')
#print(countries)

# COMMAND ----------


day=str(date.today())
print(day)

# COMMAND ----------

# DBTITLE 1,Write the rows with Nulls into Storage
dfnullAthletes.show()
try:
    dfnullAthletes.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/nullofathlete/"+day)
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

# DBTITLE 1,Write the rows with Nulls into storage
try:
    dfnullAthletes.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/AthletesNull/"+day)
except Exception as e:
    print(e)
    errorMsg = str(e)
    dict = [{'Error Message': errorMsg}]
    dfEMessage = spark.createDataFrame(dict)
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') 
    dfErrorLog = dfEMessage.withColumn('time',unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    #dfErrorLog.show(truncate = False)
    dfErrorLog.write.save(path="abfss://"+container+"@"+storage+".dfs.core.windows.net/errorLog",format='csv',mode='append',sep='\t') 
    #dfErrorLog1=dfErrorLog.union(dfErrorLog).show()

# COMMAND ----------

# DBTITLE 1,logging errors into errorLog file in storage account for teams
try:
    dfnullTeams.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/TeamsNull/"+day)
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)
    

# COMMAND ----------

# DBTITLE 1,Write the rows with Nulls into Storage for medals
try:
    dfnullMedals.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/MeadalsNull/"+day)
except Exception as e:
    print(e)
    errorMsg = str(e)
    dict = [{'Error Message': errorMsg}]
    dfEMessage = spark.createDataFrame(dict)
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') 
    dfErrorLog = dfEMessage.withColumn('time',unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    #dfErrorLog.show(truncate = False)
    dfErrorLog.write.save(path="abfss://"+container+"@"+storage+".dfs.core.windows.net/errorLog",format='csv',mode='append',sep='\t') 
    #dfErrorLog1=dfErrorLog.union(dfErrorLog).show()

# COMMAND ----------

# DBTITLE 1,Write the rows with Blanks into Storage for medals
try:
    dfMedalsblank.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/MedalsBlank/"+day)
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

# DBTITLE 1,Write the rows with Blanks into Storage for athletes
try:
    dfAthletesblank.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/AthletesBlank/"+day)
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

# DBTITLE 1,Write the rows with Blanks into Storage for teams
try:
    dfTeamsblank.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/TeamsBlank/"+day)
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

# DBTITLE 1,Unique Check
try:
    dfAthletesDistinct = dfAthletes.distinct() #Distinct Records of Athlete Table
except Exception as e:
    print(e)
    ErrorLog(errorMsg)

# COMMAND ----------

#Finding duplicate records for Athlete Table
try:
    dfAthletesDupIndicator = dfAthletes.join(dfAthletes.groupBy(dfAthletes.columns).agg((f.count("*")>1).cast("int").alias("Duplicate_indicator")), on=dfAthletes.columns, how="inner")
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

try:
    dfAthletesDuplicate = dfAthletesDupIndicator.filter("Duplicate_indicator > 0").distinct().drop("Duplicate_indicator") #Duplicate Records of Athlete Table
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

try:
    dfAthletesDuplicate.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/AthleteDuplicate/"+day)
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

try:
    dfMedalsDistinct = dfMedals.distinct() #Distinct records of Medals Table
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

#Finding duplicate records for Medals Table
dfMedalsDupIndicator = dfMedals.join(
    dfMedals.groupBy(dfMedals.columns).agg((f.count("*")>1).cast("int").alias("Duplicate_indicator")),
    on=dfMedals.columns,
    how="inner"
)

# COMMAND ----------

dfMedalsDuplicate = dfMedalsDupIndicator.filter("Duplicate_indicator > 0").distinct().drop("Duplicate_indicator") #Duplicate Records of Medals Table

# COMMAND ----------

dfMedalsDuplicate.show()

# COMMAND ----------

dfMedalsDuplicate.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/MedalsDuplicate/"+day)

# COMMAND ----------

dfTeamsDistinct = dfTeams.distinct() #Distinct records of Teams Table

# COMMAND ----------

#Finding duplicate records for Teams Table
dfTeamsDupIndicator = dfTeams.join(
    dfTeams.groupBy(dfTeams.columns).agg((f.count("*")>1).cast("int").alias("Duplicate_indicator")),
    on=dfTeams.columns,
    how="inner"
)

# COMMAND ----------

dfTeamsDuplicate = dfTeamsDupIndicator.filter("Duplicate_indicator > 0").distinct().drop("Duplicate_indicator") #Duplicate Records of Medals Table

# COMMAND ----------

try:
    dfTeamsDuplicate.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/TeamsDuplicate/"+day)
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)
    

# COMMAND ----------

# DBTITLE 1,Validity checks
dfAthletesAlnum=dfAthletes.withColumn("al",reduce(lambda x, y: x | y, (f.col(x).cast("int").isNotNull() for x in dfAthletes.columns)))
dfAthletesAlnum.show()

# COMMAND ----------

dfAthletesAlnum.filter(f.col("al")==True).show()

# COMMAND ----------

dfAthletesValidity=dfAthletesAlnum.where("al")

# COMMAND ----------

dfAthletesValidity.show()

# COMMAND ----------

dfMedalsAlnum=dfMedals.withColumn("al", f.col("Team/NOC").cast("int").isNotNull())

# COMMAND ----------

dfMedalsValidity = dfMedalsAlnum.filter("al") 

# COMMAND ----------

dfMedalsValidity.show()

# COMMAND ----------

dfTeamsAlnum=dfTeams.withColumn("al",reduce(lambda x, y: x | y, (f.col(x).cast("int").isNotNull() for x in dfTeams.columns)))


# COMMAND ----------

dfTeamsValidity = dfTeamsAlnum.where("al")

# COMMAND ----------

dfTeamsValidity.show()

# COMMAND ----------

try:
    dfTeamsValidity.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/TeamsValidity/"+day)
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)
    

# COMMAND ----------

try:
    dfAthletesValidity.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/AthletesValidity/"+day)
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)
    

# COMMAND ----------

try:
    dfMedalsValidity.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/MedalsValidity/"+day)
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)
    

# COMMAND ----------

# DBTITLE 1,Error Logging


# COMMAND ----------

# DBTITLE 1,Defining max values for column
numberdict={"Gold":50,"Silver":50,"Bronze":50,"Total":150,"Rank By Total":100}#dictionary which has the maximum value an integer column can have

# COMMAND ----------

# DBTITLE 1,Accuracy: Number Check
dfAcNCFmedals=dfMedals.filter((f.col("Gold")>numberdict["Gold"])|(f.col("Silver")>numberdict["Silver"])|(f.col("Bronze")>numberdict["Bronze"])|(f.col("Total")>numberdict["Total"])|(f.col("Rank By Total")>numberdict["Rank By Total"]))#accuracy number check failed valued
dfAcNCFmedals.show()

# COMMAND ----------

# DBTITLE 1,Write the rows with number check failure to Storage
try:
    dfAcNCFmedals.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/AcNCFmedals/"+day)
    
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)

# COMMAND ----------

# DBTITLE 1,Getting Athlete data without errors(currently Nulls, Blanks, Validity and Duplicates)
dfAthletesNew=dfAthletes.subtract(dfnullAthletes.union(dfAthletesblank.union(dfAthletesValidity.drop("al")))).distinct()#give union of all error data inside subtract
dfAthletesNew.show()

# COMMAND ----------

# DBTITLE 1,Getting Medal data without errors(currently Nulls, Blanks, Validity, Number Check and Duplicates)
dfMedalsNew=dfMedals.subtract(dfnullMedals.union(dfMedalsblank.union(dfMedalsValidity.drop("al").union(dfAcNCFmedals)))).distinct()#give union of all error data inside subtract 
dfMedalsNew.show()

# COMMAND ----------

# DBTITLE 1,Getting Team data without errors(currently Nulls, Blanks, Validity and Duplicates)
dfTeamsNew=dfTeams.subtract(dfnullTeams.union(dfTeamsblank.union(dfTeamsValidity.drop("al")))).distinct()#give union of all error data inside subtract
dfTeamsNew.show()

# COMMAND ----------

# DBTITLE 1,Initializing Error Dataframe
emptyRDD = spark.sparkContext.emptyRDD()
schema = StructType([
  StructField('TableName', StringType(), True),
  StructField('Column', StringType(), True),
  StructField('CheckType', StringType(), True)
  ])
dfError=spark.createDataFrame(emptyRDD,schema)

# COMMAND ----------

# DBTITLE 1,Putting the Percentage of the Null and Blank Errors in a Dataframe
errorschema=StructType([
    StructField('TableName',StringType(),True),
    StructField('ErrorPercentage',FloatType(),True),
    StructField('TypeofError',StringType(),True)
])
dfErrorPer=spark.createDataFrame([('Athletes',round((dfnullAthletes.count()/dfAthletes.count())*100,3),'Null Error'),('Medals',round((dfnullMedals.count()/dfMedals.count())*100,3),'Null Error'),('Teams',round((dfnullTeams.count()/dfTeams.count())*100,3),'NullError'),('Athletes',round((dfAthletesblank.count()/dfAthletes.count())*100,3),'Blank Error'),('Medals',round((dfMedalsblank.count()/dfMedals.count())*100,3),'Blank Error'),('Teams',round((dfTeamsblank.count()/dfTeams.count())*100,3),'Blank Error'),('Athletes',round((dfAthletesDuplicate.count()/dfAthletes.count())*100,3),'Duplicate Error'),('Medals',round((dfMedalsDuplicate.count()/dfMedals.count())*100,3),'Duplicate Error'),('Teams',round((dfTeamsDuplicate.count()/dfTeams.count())*100,3),'Duplicate Error'),('Athletes',round((dfAthletesValidity.count()/dfAthletes.count())*100,3),'Validity Error'),('Medals',round((dfMedalsValidity.count()/dfMedals.count())*100,3),'Validity Error'),('Teams',round((dfTeamsValidity.count()/dfTeams.count())*100,3),'Validity Error'),('Athletes',0.0,'Accuracy Number Check Error'),('Medals',round((dfAcNCFmedals.count()/dfMedals.count())*100,3),'Accuracy Number Check Error'),('Teams',0.0,'Accuracy Number Check Error'),],errorschema)

# COMMAND ----------

dfErrorPer.show()

# COMMAND ----------

errorschema1=StructType([
    StructField('TableName',StringType(),True),
    StructField('TypeofError',StringType(),True),
    StructField('ActualCount',IntegerType(),True),
    StructField('Error count',IntegerType(),True),
    StructField('cleandata count',IntegerType(),True),
    StructField('ErrorPercentage',FloatType(),True)
])
dfErrorstats=spark.createDataFrame([
    ('Athletes','Null Error',dfAthletes.count(),dfnullAthletes.count(),dfAthletes.count()-dfnullAthletes.count(),round((dfnullAthletes.count()/dfAthletes.count())*100,3)),
    ('Medals','Null Error',dfMedals.count(),dfnullMedals.count(),dfMedals.count()-dfnullMedals.count(),round((dfnullMedals.count()/dfMedals.count())*100,3)),
    ('Teams','Null Error',dfTeams.count(),dfnullTeams.count(),dfTeams.count()-dfnullTeams.count(),round((dfnullTeams.count()/dfTeams.count())*100,3)),
    ('Athletes','Blank Error',dfAthletes.count(),dfAthletesblank.count(),dfAthletes.count()-dfAthletesblank.count(),round((dfAthletesblank.count()/dfAthletes.count())*100,3)),
    ('Medals','Blank Error',dfMedals.count(),dfMedalsblank.count(),dfMedals.count()-dfMedalsblank.count(),round((dfMedalsblank.count()/dfMedals.count())*100,3)),
    ('Teams','Blank Error',dfTeams.count(),dfTeamsblank.count(),dfTeams.count()-dfTeamsblank.count(),round((dfTeamsblank.count()/dfTeams.count())*100,3)),
    ('Athletes','Duplicate Error',dfAthletes.count(),dfAthletesDuplicate.count(),dfAthletes.count()-dfAthletesDuplicate.count(),round((dfAthletesDuplicate.count()/dfAthletes.count())*100,3)),
    ('Medals','Duplicate Error',dfMedals.count(),dfMedalsDuplicate.count(),dfMedals.count()-dfMedalsDuplicate.count(),round((dfMedalsDuplicate.count()/dfMedals.count())*100,3)),
    ('Teams','Duplicate Error',dfTeams.count(),dfTeamsDuplicate.count(),dfTeams.count()-dfTeamsDuplicate.count(),round((dfTeamsDuplicate.count()/dfTeams.count())*100,3)),
    ('Athletes','Validity Error',dfAthletes.count(),dfAthletesValidity.count(),dfAthletes.count()-dfAthletesValidity.count(),round((dfAthletesValidity.count()/dfAthletes.count())*100,3)),
    ('Medals','Validity Error',dfMedals.count(),dfMedalsValidity.count(),dfMedals.count()-dfMedalsValidity.count(),round((dfMedalsValidity.count()/dfMedals.count())*100,3)),
    ('Teams','Validity Error',dfTeams.count(),dfTeamsValidity.count(),dfTeams.count()-dfTeamsValidity.count(),round((dfTeamsValidity.count()/dfTeams.count())*100,3)),
    ('Athletes','Accuracy Number Check Error',0,0,0,0.0),
    ('Medals','Accuracy Number Check Error',dfMedals.count(),dfAcNCFmedals.count(),dfMedals.count()-dfAcNCFmedals.count(),round((dfAcNCFmedals.count()/dfMedals.count())*100,3)),
    ('Teams','Accuracy Number Check Error',0,0,0,0.0),],errorschema1)

# COMMAND ----------

dfErrorstats.show()

# COMMAND ----------

dfErrorstats.select("TypeofError","Error count").groupBy("TypeofError").sum("Error count").show()

# COMMAND ----------

df = dfErrorstats.select("TypeofError","Error count").groupBy("TypeofError").sum("Error count").toPandas()
df.plot(kind = 'bar',
        x = 'TypeofError',
        y = 'sum(Error count)',
        color = 'green')

plt.title('Bar Chart for the number of Types of Error Encountered')
plt.xlabel('Type of Error')
plt.ylabel('Total Error Count')
plt.show()

# COMMAND ----------

dfErrorstats.select("TypeofError","Error count").groupBy("TypeofError").sum("Error count").show()

# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
connect_str = "DefaultEndpointsProtocol=https;AccountName=capstonebr;AccountKey=ac5pMU5ZuyIDeTiJFz3YiYQdumUp2OruitzNHaaX+cf3ZHOHLm0rGOYhuibtlmBg/edhSiDY1ExQlzd2o+eEDg==;EndpointSuffix=core.windows.net"
containerobj=ContainerClient.from_container_url("https://capstonebr.blob.core.windows.net/capstonedata?sp=rl&st=2021-11-03T05:35:16Z&se=2021-11-10T13:35:16Z&spr=https&sv=2020-08-04&sr=c&sig=Uv3ctfLcye1I0HoiDUQc2bVIBTafp9yI0ZKbhgLNurs%3D")
blob_list=containerobj.list_blobs(name_starts_with=None, include=None)
listofinput=[blob.name for blob in blob_list if '.csv' in blob.name and 'part' not in blob.name]
print(listofinput)

# COMMAND ----------

dbutils.widgets.multiselect("Choose Tables needed",listofinput[0],listofinput)

# COMMAND ----------

tables=dbutils.widgets.get("Choose Tables needed")
print(tables)
tables=tables.split(',')
print(tables)

# COMMAND ----------

df=spark.read.format("csv").option("header","true").load("abfss://"+container+"@"+storage+".dfs.core.windows.net/"+tables[1])
df.show()

# COMMAND ----------

columnlist=[x for x in df.columns if not x.startswith('_c')]
print(columnlist)

# COMMAND ----------

print(columnlist[1])

# COMMAND ----------

dbutils.widgets.dropdown(tables[1]+" join",columnlist[1],columnlist,"Column to use for join in "+tables[1])
dbutils.widgets.multiselect(tables[1]+" numbercheck",columnlist[0],columnlist,"Column which are numbers "+tables[1])

# COMMAND ----------

colnc=dbutils.widgets.get(tables[1]+" numbercheck")
colnc=colnc.split(',')
print(colnc)

# COMMAND ----------

df.select(colnc).show()

# COMMAND ----------

for i in range(0,len(colnc)):
    dbutils.widgets.text("max"+colnc[i],'0')

# COMMAND ----------

maxdict={}
for i in range(0,len(colnc)):
    maxdict[colnc[i]]=dbutils.widgets.get("max"+colnc[i])
print(maxdict)

# COMMAND ----------

dfAcNCF=df.select(columnlist).where(reduce(lambda x,y:x | y,[(f.col(i)>int(maxdict[i])) for i in maxdict.keys()]))#accuracy number check failed valued
dfAcNCF.show()

# COMMAND ----------

reduce(lambda x,y:x | y,[(f.col(i)>int(maxdict[i])) for i in maxdict.keys()])

# COMMAND ----------

# DBTITLE 1,Untitled
from country_list import countries_for_language
countries=countries_for_language('en')
print(countries)

# COMMAND ----------

newcont=[]
for c in countries:
    newcont.append(c[1])
print(newcont)

# COMMAND ----------

newcont.append("ROC")
newcont.append("Great Britain")
newcont.append("Republic of Korea")
newcont.append("Czech Republic")
newcont.append("Laos")
newcont.append("Virgin Islands, US")
newcont.append("Kosovo")
print(newcont)

# COMMAND ----------

dfAthletesNew.show()

# COMMAND ----------

noclist=[x['NOC'] for x in dfAthletesNew.collect()]
noclist=set(noclist)
print(noclist)

# COMMAND ----------

nomatch=[]
for i in noclist:
    j=process.extractOne(i,newcont,score_cutoff=80)
    if j!=None:
        if j[1]!=100:
            print(i,j)
            dfAthletesNew=dfAthletesNew.replace(i,j)
    else:
        nomatch.append(i)
print("\nThese don't have a match:")
for i in nomatch:
    print(">",i)

#fuzz.ratio("spaiN","Spain") > 80

# COMMAND ----------


dfAthletesNew.limit(dfAthletesNew.count()).where(f.col("NOC").like("%United%")).show()

# COMMAND ----------

teamnoclist=[x['Team/NOC'] for x in dfMedalsNew.collect()]
teamnoclist=set(teamnoclist)
print(teamnoclist)

# COMMAND ----------

nomatch=[]
for i in teamnoclist:
    j=process.extractOne(i,newcont,score_cutoff=80)
    if j!=None:
        if j[1]!=100:
            print(i,j)
            dfMedalsNew=dfMedalsNew.replace(i,j)
    else:
        nomatch.append(i)
print("\nThese don't have a match:")
for i in nomatch:
    print(">",i)


# COMMAND ----------

teamlist=[x['NOC'] for x in dfTeamsNew.collect()]
teamlist=set(teamlist)
print(teamlist)

# COMMAND ----------

nomatch=[]
for i in teamlist:
    j=process.extractOne(i,newcont,score_cutoff=80)
    if j!=None:
        if j[1]!=100:
            print(i,j)
            dfTeamsNew=dfTeamsNew.replace(i,j)
    else:
        nomatch.append(i)
print("\nThese don't have a match:")
for i in nomatch:
    print(">",i)


# COMMAND ----------

dfTeamsNew.show()

# COMMAND ----------

dfAthletesTemp=dfAthletesNew.toDF("Name","NOC","Discipline")
dfAthletesTemp.show()

# COMMAND ----------

dfjoin1=dfTeamsNew.join(dfAthletesTemp, dfTeamsNew.NOC==dfAthletesTemp.ANOC,"inner")

# TeamsNewVie

# COMMAND ----------

print(dfTeamsNew.count()*dfAthletesTemp.count())

# COMMAND ----------

dfjoin1.rdd.getNumPartition()

# COMMAND ----------

dfjoin1.first()

# COMMAND ----------


dfAthletesNew.createOrReplaceTempView("athlete")
spark.sql("SELECT * FROM athlete").show(10)

# COMMAND ----------

dfTeamsNew.createOrReplaceTempView("team")

# COMMAND ----------

spark.sql("SELECT * FROM team").show()

# COMMAND ----------

spark.sql("SELECT * FROM athlete INNER JOIN team ON athlete.NOC=team.NOC").show()

# COMMAND ----------

from pyspark.sql.functions import broadcast

# COMMAND ----------

dfMedalsNew=dfMedalsNew.toDF("Rank","NOC","Gold","Silver","Bronze","Total","RankbyTotal")
dfMedalsNew.show()


# COMMAND ----------

dfAthletesTemp=dfAthletesTemp.cache()
dfTeamsNew=dfTeamsNew.cache()
dfMedalsNew=dfMedalsNew.cache()
print(dfTeamsNew.count())
print(dfAthletesTemp.count())
print(dfMedalsNew.count())

# COMMAND ----------

dfjoin2=dfAthletesTemp.join(broadcast(dfTeamsNew), on=['NOC'],how="inner").join(broadcast(dfMedalsNew),on=['NOC'],how="inner")

# COMMAND ----------

dfjoin2=dfjoin2.cache()
print(dfjoin2.count())

# COMMAND ----------

dfjoin2.show()

# COMMAND ----------

dfjoin2.filter("total>100").count()

# COMMAND ----------

dfErrorstats.show()

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# COMMAND ----------

