# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "58ab00b0-158b-45e6-ad14-d7a6b7e7ff78",
# META       "default_lakehouse_name": "stream_delta",
# META       "default_lakehouse_workspace_id": "cfa78f6c-587c-4b90-8604-31ae9450a998",
# META       "known_lakehouses": [
# META         {
# META           "id": "58ab00b0-158b-45e6-ad14-d7a6b7e7ff78"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Delta Lake tables 
# Use this notebook to explore Delta Lake functionality 

# CELL ********************

from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType

# define the schema
schema = StructType() \
.add("ProductID", IntegerType(), True) \
.add("ProductName", StringType(), True) \
.add("Category", StringType(), True) \
.add("ListPrice", DoubleType(), True)

df = spark.read.format("csv").option("header","true").schema(schema).load("Files/products/products.csv")
# df now is a Spark DataFrame containing CSV data from "Files/products/products.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").saveAsTable("managed_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Criar tabelas Delta
# 


# MARKDOWN ********************

# As tabelas Delta gerenciadas se beneficiam de um desempenho mais alto, uma vez que o Fabric gerencia os metadados do esquema e os arquivos de dados.
# As tabelas externas permitem que você armazene dados externamente, com os metadados gerenciados pelo Fabric

# CELL ********************

df.write.format("delta").saveAsTable("external_products", path="abfss://LAB_UQ@onelake.dfs.fabric.microsoft.com/stream_delta.Lakehouse/Files/external_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 %%sql
 DESCRIBE FORMATTED managed_products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 %%sql
 DESCRIBE FORMATTED external_products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 %%sql
 DROP TABLE managed_products;
 DROP TABLE external_products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 %%sql
 CREATE TABLE products
 USING DELTA
 LOCATION 'Files/external_products';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 %%sql
 SELECT * FROM products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE products
# MAGIC SET ListPrice = ListPrice * 0.9
# MAGIC WHERE Category = 'Mountain Bikes';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE HISTORY products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delta_table_path = 'Files/external_products'
# Get the current data
current_data = spark.read.format("delta").load(delta_table_path)
display(current_data)

# Get the version 0 data
original_data = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
display(original_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 %%sql
 -- Create a temporary view
 CREATE OR REPLACE TEMPORARY VIEW products_view
 AS
     SELECT Category, COUNT(*) AS NumProducts, MIN(ListPrice) AS MinPrice, MAX(ListPrice) AS MaxPrice, AVG(ListPrice) AS AvgPrice
     FROM products
     GROUP BY Category;

 SELECT *
 FROM products_view
 ORDER BY Category;    

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 %%sql
 SELECT Category, NumProducts
 FROM products_view
 ORDER BY NumProducts DESC
 LIMIT 10;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 from pyspark.sql.functions import col, desc

 df_products = spark.sql("SELECT Category, MinPrice, MaxPrice, AvgPrice FROM products_view").orderBy(col("AvgPrice").desc())
 display(df_products.limit(6))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # _Usar tabelas Delta para streaming de dados._
# 
# O Delta Lake permite streaming de dados. As tabelas delta podem ser um coletor ou uma fonte para fluxos de dados criados por meio da API de Streaming Estruturado do Spark. Neste exemplo, você usará uma tabela Delta como um coletor para streaming de dados em um cenário simulado de IoT (Internet das Coisas).

# CELL ********************

# Adicione uma nova célula de código, o código a seguir e execute:

from notebookutils import mssparkutils
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create a folder
inputPath = 'Files/data/'
mssparkutils.fs.mkdirs(inputPath)

# Create a stream that reads data from the folder, using a JSON schema
jsonSchema = StructType([
StructField("device", StringType(), False),
StructField("status", StringType(), False)
])
iotstream = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)

# Write some event data to the folder
device_data = '''{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"ok"}
{"device":"Dev2","status":"error"}
{"device":"Dev1","status":"ok"}
{"device":"Dev1","status":"error"}
{"device":"Dev2","status":"ok"}
{"device":"Dev2","status":"error"}
{"device":"Dev1","status":"ok"}'''

mssparkutils.fs.put(inputPath + "data.txt", device_data, True)

print("Source stream created...")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# - Verifique que o texto Fluxo de origem criado… será exibida. O código que você acabou de executar criou uma fonte de dados de streaming com base em uma pasta na qual alguns dados foram salvos, representando leituras de dispositivos IoT hipotéticos.

# CELL ********************

# Write the stream to a delta table
delta_stream_table_path = 'Tables/iotdevicedata'
checkpointpath = 'Files/delta/checkpoint'
deltastream = iotstream.writeStream.format("delta").option("checkpointLocation", checkpointpath).start(delta_stream_table_path)
print("Streaming to delta sink...")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# - Esse código grava os dados do dispositivo de streaming no formato Delta em uma pasta chamada iotdevicedata. Como o caminho para o local da pasta na pasta Tabelas, uma tabela será criada automaticamente para ela.

# CELL ********************

 %%sql
 SELECT * FROM IotDeviceData;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# - Esse código consulta a tabela IotDeviceData, que contém os dados do dispositivo da fonte de streaming.

# CELL ********************

 # Add more data to the source stream
 more_data = '''{"device":"Dev1","status":"ok"}
 {"device":"Dev1","status":"error"}
 {"device":"Dev1","status":"ok"}
 {"device":"Dev1","status":"ok"}
 {"device":"Dev1","status":"error"}
 {"device":"Dev2","status":"ok"}
 {"device":"Dev1","status":"ok"}'''

 mssparkutils.fs.put(inputPath + "more-data_3.txt", more_data, True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 %%sql
 SELECT * FROM IotDeviceData;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.text("Files/data/data.txt")
# df now is a Spark DataFrame containing text data from "Files/data/data.txt".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltastream.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 %%sql
 SELECT * FROM IotDeviceData;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
