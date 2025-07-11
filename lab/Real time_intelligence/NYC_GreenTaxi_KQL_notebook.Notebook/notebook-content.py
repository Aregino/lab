# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Use a notebook with Apache Spark to query a KQL database
# 
# Documentation for use of this notebook can be found here: [Use a notebook with Apache Spark to query a KQL database](https://learn.microsoft.com/fabric/real-time-analytics/spark-connector)

# CELL ********************

# Azure storage access info
blob_account_name = "azureopendatastorage"
blob_container_name = "nyctlc"
blob_relative_path = "green"
blob_sas_token = r""

# Allow SPARK to read from Blob remotely
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set(
  'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
  blob_sas_token)
print('Remote blob path: ' + wasbs_path)

# SPARK read parquet, note that it won't load any data yet by now
df = spark.read.parquet(wasbs_path)
# Display top 10 rows
print('Displaying top 10 rows: ')
df.printSchema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# - coloque a query uri que existe na configuração do do database, como nesse exemplo: 
# ![ML](https://learn.microsoft.com/pt-br/fabric/real-time-intelligence/media/spark-connector/query-uri.png)

# CELL ********************

#The target where this data will be written to
kustoUri = "https://trd-q4mm1m92ydn7619m8c.z7.kusto.fabric.microsoft.com"
database="nycGreenTaxi"
table="GreenTaxiData" #for example, GreenTaxiData

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#This is an example of writing data to Kusto. The source data is read as a blob into a dataframe from Azure Open Data for GreenTaxi / Limousines in NYC.
#The access token is created using the user's credential and will be used to write the data to the Kusto table GreenTaxiData, therefore the user is required 
#for 'user' privileges or above on the target database and table 'admin' privileges if the table already exists. If the table does not exist, 
#it will be created with the DataFrame schema.
df.write.format("com.microsoft.kusto.spark.synapse.datasource").\
option("kustoCluster",kustoUri).\
option("kustoDatabase",database).\
option("kustoTable", table).\
option("accessToken", mssparkutils.credentials.getToken(kustoUri)).\
option("tableCreateOptions", "CreateIfNotExist").mode("Append").save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#This is an example of Reading data from the KQL Database. Here the query retrieves the max,min fares and distances that the taxi recorded every month from the years 2014 to 2020
kustoQuery = "GreenTaxiData |  where puYear between (2014 .. 2020 ) | summarize  MaxDistance=max(tripDistance) , MaxFare = max(fareAmount) ,MinDistance=min(tripDistance) , MinFare = min(fareAmount) by puYear,puMonth | order by puYear,puMonth desc"
kustoDf  = spark.read\
            .format("com.microsoft.kusto.spark.synapse.datasource")\
            .option("accessToken", mssparkutils.credentials.getToken(kustoUri))\
            .option("kustoCluster", kustoUri)\
            .option("kustoDatabase", database) \
            .option("kustoQuery", kustoQuery).load()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kustoDf.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Resumo
# 
# Assim é possivel rodar um stream de dados utilizando spark, salvando no banco **KQL**, como mais uma alternativa 
# - Eventstream
# - Notebook com streaming de dados 

