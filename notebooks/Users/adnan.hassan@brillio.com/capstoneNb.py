# Databricks notebook source
# DBTITLE 1,Import modules
import pyspark.sql.functions as f
from functools import reduce
from datetime import date
from pyspark.sql.functions import lit,unix_timestamp
import time
import datetime
from pyspark.sql.types import StructType,StructField, StringType, FloatType



# COMMAND ----------

# DBTITLE 1,Widget Creation
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
storage="capstonebr"
container="capstonedata"
clientid="11fff788-5d3c-487c-a255-6db7f2f2cac3"
secret="phk7Q~RAnMsSN-V96u4zLNjYSMn2z.N6zR.sY"
tenantid="97984c2b-a229-4609-8185-ae84947bc3fc"

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
for x in dfMedals.columns:
    if "_c" in x:
        dfMedals=dfMedals.drop(x)
        
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
for x in dfTeams.columns:
    if "_c" in x:
        dfTeams=dfTeams.drop(x)        
dfTeams.show()

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

pip install fuzzywuzzy

# COMMAND ----------

pip install python-Levenshtein

# COMMAND ----------

from fuzzywuzzy import fuzz, process
fuzz.ratio("apain","Spain")

# COMMAND ----------

pip install country_list

# COMMAND ----------

from country_list import countries_for_language
countries=countries_for_language('en')
print(countries)

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

# DBTITLE 1,Initializing Error Dataframe
emptyRDD = spark.sparkContext.emptyRDD()
schema = StructType([
  StructField('TableName', StringType(), True),
  StructField('Column', StringType(), True),
  StructField('CheckType', StringType(), True)
  ])
dfError=spark.createDataFrame(emptyRDD,schema)

# COMMAND ----------

# DBTITLE 1,Putting the Percentage of the Null Errors in a Dataframe
errorschema=StructType([
    StructField('TableName',StringType(),True),
    StructField('ErrorPercentage',FloatType(),True),
    StructField('TypeofError',StringType(),True)
])
dfErrorPer=spark.createDataFrame([('Athletes',round((dfnullAthletes.count()/dfAthletes.count())*100,3),'Null Error'),('Medals',round((dfnullMedals.count()/dfMedals.count())*100,3),'Null Error'),('Teams',round((dfnullTeams.count()/dfTeams.count())*100,3),'NullError'),('Athletes',round((dfAthletesblank.count()/dfAthletes.count())*100,3),'Blank Error'),('Medals',round((dfMedalsblank.count()/dfMedals.count())*100,3),'Blank Error'),('Teams',round((dfMedalsblank.count()/dfMedals.count())*100,3),'Blank Error')],errorschema)

# COMMAND ----------

dfErrorPer.show()

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

