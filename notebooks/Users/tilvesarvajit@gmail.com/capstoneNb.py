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
storage="capstonebrillio"
container="capstonedata"
clientid="da25c085-97e4-48a6-9ab0-7d40f0d6aee2"
secret="hzH7Q~al344Xh.P5HIweKtYeZwR30rh5O63Uw"
tenantid="2b02a57b-28d7-425c-88af-b52b3df9a6d7"

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id."+storage+".dfs.core.windows.net", clientid)
spark.conf.set("fs.azure.account.oauth2.client.secret."+storage+".dfs.core.windows.net", secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint."+storage+".dfs.core.windows.net", "https://login.microsoftonline.com/"+tenantid+"/oauth2/token")


# COMMAND ----------

# DBTITLE 1,Load Athletes
dfAthletes = spark.read.format("csv").option("header","true").load("abfss://"+container+"@"+storage+".dfs.core.windows.net/Athletes.csv")

# COMMAND ----------

dfAthletes.show()



# COMMAND ----------

# DBTITLE 1,Load Medals
dfMedals = spark.read.format("csv").option("header","true").load("abfss://"+container+"@"+storage+".dfs.core.windows.net/Medals.csv")

# COMMAND ----------

dfMedals.show()

# COMMAND ----------

# DBTITLE 1,Load Teams
dfTeams = spark.read.format("csv").option("header","true").load("abfss://"+container+"@"+storage+".dfs.core.windows.net/Teams.csv")

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
dfnullmedal=dfMedals.where(reduce(lambda x, y: x | y, (f.col(x).isNull() | (f.col(x)=="null") for x in dfMedals.columns)))
# Reduce-> implementing function across a sequence and in this case funct would be returning true if any of the value in the sequence is true which means atleast one of the column value is null
dfnullmedal.show()

# COMMAND ----------

# DBTITLE 1,Getting blank rows from Medals and replacing with NA
dfMedalsblank=dfMedals.where(reduce(lambda x, y: x | y, (f.col(x).isNull() for x in dfMedals.columns)))
dfMedalsblank.na.fill(value="NA").show()


# COMMAND ----------

# DBTITLE 1,getting rows from Medals with string null
dfnullMedals=dfMedals.where(reduce(lambda x, y: x | y, (f.col(x)=="null" for x in dfMedals.columns)))
dfnullMedals.show()

# COMMAND ----------

# DBTITLE 1,Get rows from Athletes which have atleast one null value in it
#dfnullathlete=dfAthletes.where(reduce(lambda x, y: x | y, (f.col(x).isNull() | (f.col(x)=="null") for x in dfAthletes.columns)))
# Reduce-> implementing function across a sequence and in this case funct would be returning true if any of the value in the sequence is true which means atleast one of the column value is null
#dfnullathlete.show()

# COMMAND ----------

# DBTITLE 1,Getting blank rows from the Athletes and replacing with NA
dfAthletesblank=dfAthletes.where(reduce(lambda x, y: x | y, (f.col(x).isNull() for x in dfAthletes.columns)))
dfAthletesblank.na.fill(value="NA").show()

# COMMAND ----------

# DBTITLE 1,getting rows from Athletes with string null 
dfnullAthletes=dfAthletes.where(reduce(lambda x, y: x | y, (f.col(x)=="null" for x in dfAthletes.columns)))
dfnullAthletes.show()

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
dfnullAthletes.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/nullofathlete/"+day)

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
    dict = [{'Error Message': errorMsg}]
    dfEMessage = spark.createDataFrame(dict)
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') 
    dfNewErrorLog = dfEMessage.withColumn('time',unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    #dfErrorLog.show(truncate = False)
    dfErrorLog=spark.read.format("csv").option("header","True").load("abfss://"+container+"@"+storage+".dfs.core.windows.net/errorLog")
    dfErrorLog.show()
    dfErrorLog=dfErrorLog.union(dfNewErrorLog)
    dfErrorLog.show()
    dfErrorLog.coalesce(1).write.save(path="abfss://"+container+"@"+storage+".dfs.core.windows.net/errorLog",
                                         format="csv",mode="overwrite") 
    #dfErrorLog1=dfErrorLog.union(dfErrorLog).show()
    

# COMMAND ----------

# DBTITLE 1,Write the rows with Nulls into Storage for medals
try:
    dfnullMedals.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/MeadalsNull/"+day)
except Exception as e:
    print(e)
    errorMsg = str(e)
    ErrorLog(errorMsg)
    
    


# COMMAND ----------

# DBTITLE 1,Write the rows with Blanks into Storage for medals
dfMedalsblank.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/MedalsBlank/"+day)

# COMMAND ----------

# DBTITLE 1,Write the rows with Blanks into Storage for athletes
dfAthletesblank.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/AthletesBlank/"+day)

# COMMAND ----------

# DBTITLE 1,Write the rows with Blanks into Storage for teams
dfTeamsblank.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/TeamsBlank/"+day)

# COMMAND ----------

# DBTITLE 1,Getting Athlete data without errors(currently only null and blank)
dfAthletesNew=dfAthletes.subtract(dfnullAthletes.union(dfAthletesblank))#give union of all error data inside subtract
dfAthletesNew.show()

# COMMAND ----------

# DBTITLE 1,Getting Medal data without errors(currently only null and blank)
dfMedalsNew=dfMedals.subtract(dfnullMedals.union(dfMedalsblank))#give union of all error data inside subtract 
dfMedalsNew.show()

# COMMAND ----------

# DBTITLE 1,Getting Team data without errors(currently only null and blank)
dfTeamsNew=dfTeams.subtract(dfnullTeams.union(dfTeamsblank))#give union of all error data inside subtract
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


# COMMAND ----------

#Finding duplicate records for Athlete Table
try:
    dfAthletesDupIndicator = dfAthletes.join(dfAthletes.groupBy(dfAthletes.columns).agg((f.count("*")>1).cast("int").alias("Duplicate_indicator")), on=dfAthletes.columns, how="inner")
except Exception as e:
    print(e)


# COMMAND ----------

try:
    dfAthletesDuplicate = dfAthletesDupIndicator.filter("Duplicate_indicator > 0").distinct().drop("Duplicate_indicator") #Duplicate Records of Athlete Table
except Exception as e:
    print(e)

# COMMAND ----------

dfAthletesDuplicate.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/AthleteDuplicate/"+day)

# COMMAND ----------

dfMedalsDistinct = dfMedals.distinct() #Distinct records of Medals Table

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

dfTeamsDuplicate.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/TeamsDuplicate/"+day)

# COMMAND ----------

# DBTITLE 1,Validity checks
dfAthletesAlnum=dfAthletes.withColumn("al",reduce(lambda x, y: x | y, (f.col(x).cast("int").isNotNull() for x in dfAthletes.columns)))
dfAthletesAlnum.show()

# COMMAND ----------

dfAthletesAlnum.filter(f.col("al")==True).show()

# COMMAND ----------

dfAthletesValidity=dfAthletesValidity.where("al")

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

# DBTITLE 1,Error Logging


# COMMAND ----------

# DBTITLE 1,Defining max values for column
numberdict={"Gold":50,"Silver":50,"Bronze":50,"Total":150,"Rank By Total":100}#dictionary which has the maximum value an integer column can have

# COMMAND ----------

# DBTITLE 1,Accuracy: Number Check
dfAcNCFmedals=dfMedalsNew.filter((f.col("Gold")>numberdict["Gold"])|(f.col("Silver")>numberdict["Silver"])|(f.col("Bronze")>numberdict["Bronze"])|(f.col("Total")>numberdict["Total"])|(f.col("Rank By Total")>numberdict["Rank By Total"]))#accuracy number check failed valued
dfAcNCFmedals.show()

# COMMAND ----------

# DBTITLE 1,Write the rows with number check failure to Storage
dfAcNCFmedals.write.csv("abfss://"+container+"@"+storage+".dfs.core.windows.net/AcNCFmedals/"+day)

# COMMAND ----------

