# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql.functions import when

# COMMAND ----------

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

dfdecimalcheck=df.withColumn("Population",f.floor("POPULATION"))

# COMMAND ----------

dfdecimalcheck.show()

# COMMAND ----------

# DBTITLE 1,Datecheck
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

df1=spark.read.format("csv").option("header","true").load("abfss://"+container+"@"+storage+".dfs.core.windows.net/medals1.csv")

# COMMAND ----------

pip install word2number

# COMMAND ----------

# DBTITLE 1,Consistency check(numeric word to number)
from word2number import w2n
from functools import reduce

# COMMAND ----------

df1.show()

# COMMAND ----------

dfnull=df1.where(reduce(lambda x, y: x | y, (f.col(x).isNull() | (f.col(x)=="null") for x in df1.columns)))
df2=df1.subtract(dfnull)
df2=df2.toPandas()
df2.head(50)

# COMMAND ----------


columnlist=list(df2)
for i in columnlist:
    try:
        df2[i] = df2[i].apply(w2n.word_to_num)
    except Exception as e:
        print(e)
df2.head(50)

# COMMAND ----------

#df1.withColumn("Gold",when(f.col("Gold").cast("int").isNull ,w2n.word_to_num(f.col("Gold"))).otherwise(f.col("Gold")))
#dfnum=df1.withColumn("num",reduce(lambda x, y: x | y, (f.col(x).cast("int").isNull() for x in df1.columns)))
import pyspark.sql.functions as f
cols=["name","age","Rank"]
vals=[("A","6","1"),("B","seven","two"),("C","8","6"),("D","five","4"),("E","1","six")]
dfdum = spark.createDataFrame(vals, cols)
#dfdum.show()
#dfdum.withColumn("age", when(f.col("age"),w2n.word_to_num(f.col("age"))).otherwise()
dfdumpandas=dfdum.toPandas()
#dfdumpandas['age']=dfdumpandas['age'].fillna('zero')
#dfdumpandas
collist=["age","Rank"]
for i in collist:
    dfdumpandas[i] = dfdumpandas[i].apply(w2n.word_to_num)  
dfdumpandas
        

# COMMAND ----------

# DBTITLE 1,Email Check
dfcontacts = spark.read.format("csv").option("header","true").load("abfss://"+container+"@"+storage+".dfs.core.windows.net/100-contacts.csv")

# COMMAND ----------

dfcontacts.show(truncate=False)

# COMMAND ----------

email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'

# COMMAND ----------

dfemailvalidity=dfcontacts.withColumn("validity",when(f.col("email").rlike(email_pattern),"valid").otherwise("invalid"))

# COMMAND ----------

dfemailvalidity.show()

# COMMAND ----------

dfinvalidemails= dfemailvalidity.filter(f.col("validity")=="invalid")

# COMMAND ----------

dfinvalidemails.show(truncate=False)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,StringLength Check
dfcontacts.where((f.length(f.col("address")) <5) | (f.length(f.col("address"))>30)).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Ecryption of Sensitive Data
import base64
dfPassData = spark.read.format("csv").option("header","true").load("abfss://"+container+"@"+storage+".dfs.core.windows.net/passwordData.csv")
dfPassDataPD = dfPassData.toPandas()
dfPassDataPD

# COMMAND ----------

for index, row in dfPassDataPD.iterrows():
    row['password'] = base64.b64encode(row['password'].encode("utf-8"))
  

# COMMAND ----------

dfPassDataPD

# COMMAND ----------

for index, row in dfPassDataPD.iterrows():
    row['password'] = base64.b64decode(row['password'])

# COMMAND ----------

dfPassDataPD #Decoded Data

# COMMAND ----------

# DBTITLE 1,Fuzzy Check
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