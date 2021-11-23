# Databricks notebook source
pip install azure.storage.blob

# COMMAND ----------

# DBTITLE 1,Importing Libraries
import pyspark.sql.functions as f
from functools import reduce
from datetime import date
from pyspark.sql.functions import lit,unix_timestamp
import time
import datetime
from pyspark.sql.types import StructType,StructField, StringType, FloatType,IntegerType
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import matplotlib.pyplot as plt

# COMMAND ----------

connect_str = "DefaultEndpointsProtocol=https;AccountName=capstonebatch1;AccountKey=P6MOQsMJmjg3rH4J0mkh+/lUIpY7cUkpV1aF8sW+vwXjePK/sXyv/iXnmI4B0/MkbgOSjgplvzXmhyPqrwjnsw==;EndpointSuffix=core.windows.net"
containerobj=ContainerClient.from_container_url("https://capstonebatch1.blob.core.windows.net/capstone?sp=l&st=2021-11-15T06:14:58Z&se=2022-11-15T14:14:58Z&spr=https&sv=2020-08-04&sr=c&sig=C6dqpWPNp1iR75%2F692%2BcjtjVVHgGjj4FVtwKvQnH8dk%3D")
blob_list=containerobj.list_blobs(name_starts_with=None, include=None)
listofinput=[blob.name for blob in blob_list if '.csv' in blob.name and 'part' not in blob.name]
print(listofinput)


# COMMAND ----------

# DBTITLE 1,Widget Creation
dbutils.widgets.removeAll()
dbutils.widgets.multiselect("Choose Tables needed",listofinput[0],listofinput)
dbutils.widgets.text("storage","storage name")
dbutils.widgets.text("container","container name")
dbutils.widgets.text("clientid","client_id")
#dbutils.widgets.text("secret","secret key")
dbutils.widgets.text("tenantid","tenant_id")
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
fileName = dbutils.widgets.get("Choose Tables needed")
print(fileName)
#extracting secret from kayvault
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

# DBTITLE 1,Setting Config.
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+storage+".dfs.core.windows.net", clientid)
spark.conf.set("fs.azure.account.oauth2.client.secret."+storage+".dfs.core.windows.net", secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+storage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")

# COMMAND ----------

# DBTITLE 1,Loading Data
def LoadData(fileName):
    try:
        return spark.read.format("csv").option("header","true").option("inferSchema", "true").load("abfss://"+container+"@"+storage+".dfs.core.windows.net/"+fileName)
    except Exception as e:
        print(e)
        errorMsg = str(e)
        ErrorLog(errorMsg)
dataFrame = LoadData(fileName)
dataFrame.show(5)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Function to get dataframe without unwanted columns
def UnwantedCol(dataFrame):
    try:
        for x in dataFrame.columns:
            if "_c" in x:
                dataFrame=dataFrame.drop(x)
            
        return dataFrame
    except Exception as e:
        print(e)
        errorMsg = str(e)
        ErrorLog(errorMsg)

dataFrameStripped = UnwantedCol(dataFrame) #Unwanted Columns stripped

# COMMAND ----------

# DBTITLE 1,Handling Blanks & Nulls
def nullCheck(dataFrameStripped): #Returns a dataframe with Blanks filled with NA
    try:
        dataFrameBlanks = dataFrameStripped.where(reduce(lambda x, y: x | y, (f.col(x).isNull() for x in dataFrameStripped.columns)))
        return dataFrameBlanks
    except Exception as e:
        print(e)
        errorMsg = str(e)
        ErrorLog(errorMsg)
        
NullsCheck = nullCheck(dataFrameStripped)

# COMMAND ----------

# DBTITLE 1,Function to get distinct rows
def GetDistinct(dataFrameStripped):
    try:
        dfDistinct = dataFrameStripped.distinct() #Distinct Records of Athlete Table    
        return dfDistinct
    except Exception as e:
        print(e)
        errorMsg = str(e)
        ErrorLog(errorMsg)

DistinctDF= GetDistinct(dataFrameStripped)

# COMMAND ----------

# DBTITLE 1,Function to get Duplicate Rows
def GetDuplicate(dataFrameStripped):
    try:
        dfDupIndicator = dataFrameStripped.join(dataFrameStripped.groupBy(dataFrameStripped.columns).agg((f.count("*")>1).cast("int").alias("Duplicate_indicator")), on=dataFrameStripped.columns, how="inner")
        return dfDupIndicator
    except Exception as e:
        print(e)
        errorMsg = str(e)
        ErrorLog(errorMsg)

DuplicateCheck = GetDuplicate(dataFrameStripped)

# COMMAND ----------

# DBTITLE 1,Function to validate numeric data type
def ValidateNumDType(dataFrameStripped):
    try:
        ColumnList=[item[0] for item in dataFrameStripped.dtypes if item[1].startswith('string')]
        dfnum=dataFrameStripped.withColumn("num",reduce(lambda x, y: x | y, (f.col(x).cast("int").isNotNull() for x in ColumnList)))
        dfnumFilter =  dfnum.filter(f.col("num")==True).drop("num")
        return dfnumFilter
    
    except Exception as e:
        print(e)
        errorMsg = str(e)
        ErrorLog(errorMsg)
        
numValidityCheck = ValidateNumDType(dataFrameStripped)

# COMMAND ----------

# DBTITLE 1,Accuracy Check
emptyRDD = spark.sparkContext.emptyRDD()
schema = dataFrameStripped.schema
dfEmpty=spark.createDataFrame(emptyRDD,schema)

# COMMAND ----------

def accuracyCheck(MaxValue):
    try:
          
        ColumnList=[item[0] for item in dataFrameStripped.dtypes if item[1].startswith('int')]
        if ColumnList!=[]:
            accCheckDF =  dataFrameStripped.where(reduce(lambda x, y: x | y, (f.col(x)>MaxValue for x in ColumnList)))#accuracy number check failed valued
        else:
            accCheckDF = dfEmpty
        return accCheckDF
    except Exception as e:
        print(e)
        errorMsg = str(e)
        ErrorLog(errorMsg)   

# COMMAND ----------

MaxValue= 100
AccuracyCheck = accuracyCheck(MaxValue)
AccuracyCheck.show(5)

# COMMAND ----------

# DBTITLE 1,Getting data without errors(currently Nulls, Blanks, Validity and Duplicates)
def cleanData(dataFrameStripped):
    try:
        #cleanDF= dataFrameStripped.subtract(NullsCheck.union(AccuracyCheck.union(numValidityCheck))).distinct()
        nullChecked = dataFrameStripped.subtract(NullsCheck).distinct()
        AccuracyChecked = nullChecked.subtract(AccuracyCheck).distinct()
        cleanDF = AccuracyChecked.subtract(numValidityCheck).distinct()
        return cleanDF
    except Exception as e:
        print(e)
        errorMsg = str(e)
        ErrorLog(errorMsg)

# COMMAND ----------

CleanData = cleanData(dataFrameStripped)
CleanData.show(5)

# COMMAND ----------

day=str(date.today())
print(day)

fileNameInter = fileName.split(".")
fileNameF = fileNameInter[0]
fileNameF

# COMMAND ----------

# DBTITLE 1,Error percentage and Error Count
errorschema1=StructType([
    StructField('TableName',StringType(),True),
    StructField('TypeofError',StringType(),True),
    StructField('ActualCount',IntegerType(),True),
    StructField('Error count',IntegerType(),True),
    StructField('cleandata count',IntegerType(),True),
    StructField('ErrorPercentage',FloatType(),True)
])

# COMMAND ----------

def ErrorStats():
    try:
        dfErrorstats=spark.createDataFrame([(fileNameF,'Null Error',dataFrameStripped.count(),NullsCheck.count(),dataFrameStripped.count()-       NullsCheck.count(),round((NullsCheck.count()/dataFrameStripped.count())*100,3)),
    (fileNameF,'Duplicate Error',dataFrameStripped.count(),DuplicateCheck.count(),dataFrameStripped.count()-DuplicateCheck.count(),round((DuplicateCheck.count()/dataFrameStripped.count())*100,3)),
    (fileNameF,'Validity Error',dataFrameStripped.count(),numValidityCheck.count(),dataFrameStripped.count()-numValidityCheck.count(),round((numValidityCheck.count()/dataFrameStripped.count())*100,3)),
    (fileNameF,'Accuracy Number Check Error',dataFrameStripped.count(),AccuracyCheck.count(),dataFrameStripped.count()-AccuracyCheck.count(),round((AccuracyCheck.count()/dataFrameStripped.count())*100,3))],errorschema1)
        return dfErrorstats
    except Exception as e:
        ErrorLog(str(e))

# COMMAND ----------

ErrorStats= ErrorStats()

# COMMAND ----------

def ErrorGraph():
    try:
        ErrorStats.select("TypeofError","Error count").groupBy("TypeofError").sum("Error count").show()
        ErrorStats.select("TableName","ActualCount","Error count","cleandata count").groupBy("TableName").sum("ActualCount","Error count","cleandata count").show()

        df = ErrorStats.select("TypeofError","Error count").groupBy("TypeofError").sum("Error count").toPandas()
        df.plot(kind = 'bar',
                x = 'TypeofError',
                y = 'sum(Error count)',
                color = 'green')

        plt.title('Bar Chart for the number of Types of Error Encountered')
        plt.xlabel('Type of Error')
        plt.ylabel('Total Error Count')
        plt.show()

        df = ErrorStats.select("TableName","ActualCount","Error count","cleandata count").groupBy("TableName").sum("ActualCount","Error count","cleandata count").toPandas()
        df.plot(kind = 'bar')
        plt.xlabel('Different type of error')
        plt.ylabel('Total Data Count')
        plt.show()
    except Exception as e:
        ErrorLog(str(e))

# COMMAND ----------

# DBTITLE 1,Writing to Storage Blob
def writeNullsCheck():
    try:
        NullsCheck.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/"+fileNameF+"/NullsCheck/"+day)
    except Exception as e:
        ErrorLog(str(e))
def writeDuplicateCheck():
    try:
        DuplicateCheck.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/"+fileNameF+"/DuplicateCheck/"+day)
    except Exception as e:
        ErrorLog(str(e))
def writeNumValidityCheck():
    try:
        numValidityCheck.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/"+fileNameF+"/numValidityCheck/"+day)
    except Exception as e:
        ErrorLog(str(e))
def writeAccuracyCheck():
    try:
        AccuracyCheck.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/"+fileNameF+"/AccuracyCheck/"+day)
    except Exception as e:
        ErrorLog(str(e))
def writeCleanData():
    try:
        CleanData.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/"+fileNameF+"/CleanData/"+day)
    except Exception as e:
        ErrorLog(str(e))
def writeErrorStats():
    try:
        ErrorStats.coalesce(1).write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/"+fileNameF+"/ErrorStats/"+day)
    except Exception as e:
        ErrorLog(str(e))

# COMMAND ----------

writeNullsCheck()
writeDuplicateCheck()
writeNumValidityCheck()
writeAccuracyCheck()
writeCleanData()
writeErrorStats()


# COMMAND ----------

ErrorGraph()

# COMMAND ----------

def fuzzycheck(df,colname,consistantlist):
    """
    df is the dataframe in which the fuzzy check is implemented.
    colname is the column in which fuzzy check is done.
    consintantlist should have the correct data.
    """
    collist=[x[colname] for x in df.collect()]
    collist=set(collist)
    for i in collist:
        j=process.extractOne(i,consistantlist,score_cutoff=80)
        if j!=None:
            if j[1]!=100:
                df=df.replace(i,j)
    return df