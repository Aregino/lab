# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9b60c53a-9c0d-4a3c-9b71-c09695b7358d",
# META       "default_lakehouse_name": "lab03_medallion",
# META       "default_lakehouse_workspace_id": "cfa78f6c-587c-4b90-8604-31ae9450a998",
# META       "known_lakehouses": [
# META         {
# META           "id": "9b60c53a-9c0d-4a3c-9b71-c09695b7358d"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "9c5c70fb-f6a7-4f7f-a95c-6773ac8a9fa5",
# META       "known_warehouses": [
# META         {
# META           "id": "9c5c70fb-f6a7-4f7f-a95c-6773ac8a9fa5",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

# Create the schema for the table
orderSchema = StructType([
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderLineNumber", IntegerType()),
    StructField("OrderDate", DateType()),
    StructField("CustomerName", StringType()),
    StructField("Email", StringType()),
    StructField("Item", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("Tax", FloatType())
    ])

# Import all files from bronze folder of lakehouse
df = spark.read.format("csv").option("header", "true").schema(orderSchema).load("Files/bronze/*.csv")

# Display the first 10 rows of the dataframe to preview your data
display(df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# O código que você executou carregou os dados dos arquivos CSV na pasta bronze em um dataframe do Spark e, em seguida, exibiu as primeiras linhas do dataframe.
# 
# Agora você adicionará colunas para validação e limpeza de dados, usando um dataframe do PySpark para adicionar colunas e atualizar os valores de algumas das colunas existentes. Use o botão + para adicionar um novo bloco de código e adicione o seguinte código à célula:

# CELL ********************

from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name

# Add columns IsFlagged, CreatedTS and ModifiedTS
df = df.withColumn("FileName", input_file_name()) \
    .withColumn("IsFlagged", when(col("OrderDate") < '2019-08-01',True).otherwise(False)) \
    .withColumn("CreatedTS", current_timestamp()).withColumn("ModifiedTS", current_timestamp())

# Update CustomerName to "Unknown" if CustomerName null or empty
df = df.withColumn("CustomerName", when((col("CustomerName").isNull() | (col("CustomerName")=="")),lit("Unknown")).otherwise(col("CustomerName")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# A primeira linha do código que você executou importa as funções necessárias do PySpark. Em seguida, você está adicionando novas colunas ao dataframe para que possa acompanhar o nome do arquivo de origem, se o pedido foi sinalizado como sendo de antes do ano fiscal de interesse e quando a linha foi criada e modificada.
# 
# Por fim, você está atualizando a coluna CustomerName para “Desconhecido” se ela for nula ou vazia.
# 
# Execute a célula para executar o código usando o botão **▷ (Executar célula).
# 
# Em seguida, você definirá o esquema para a tabela sales_silver no banco de dados de vendas usando o formato Delta Lake. Crie um novo bloco de código e adicione o seguinte código à célula:

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW SCHEMAS;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the schema for the sales_silver table

from pyspark.sql.types import *
from delta.tables import *

DeltaTable.createIfNotExists(spark) \
    .tableName("lab03_medallion.sales_silver") \
    .addColumn("SalesOrderNumber", StringType()) \
    .addColumn("SalesOrderLineNumber", IntegerType()) \
    .addColumn("OrderDate", DateType()) \
    .addColumn("CustomerName", StringType()) \
    .addColumn("Email", StringType()) \
    .addColumn("Item", StringType()) \
    .addColumn("Quantity", IntegerType()) \
    .addColumn("UnitPrice", FloatType()) \
    .addColumn("Tax", FloatType()) \
    .addColumn("FileName", StringType()) \
    .addColumn("IsFlagged", BooleanType()) \
    .addColumn("CreatedTS", DateType()) \
    .addColumn("ModifiedTS", DateType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Execute a célula para executar o código usando o botão **▷ (Executar célula).
# 
# Selecione … na seção Tabelas do painel do lakehouse explorer e selecione Atualizar. Agora você deve ver a nova tabela sales_silver listada. O ▲ (ícone de triângulo) indica que se trata de uma tabela Delta.

# MARKDOWN ********************

# ![image](https://microsoftlearning.github.io/mslearn-fabric.pt-br/Instructions/Labs/Images/sales-silver-table.png)

# MARKDOWN ********************

# Agora você deve executar uma operação upsert em uma tabela Delta, atualizando os registros existentes com base em condições específicas e inserindo novos registros quando nenhuma correspondência for encontrada. Adicione um novo bloco de código e cole o seguinte código:

# CELL ********************

# Update existing records and insert new ones based on a condition defined by the columns SalesOrderNumber, OrderDate, CustomerName, and Item.

from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/sales_silver')
    
dfUpdates = df
    
deltaTable.alias('silver') \
  .merge(
    dfUpdates.alias('updates'),
    'silver.SalesOrderNumber = updates.SalesOrderNumber and silver.OrderDate = updates.OrderDate and silver.CustomerName = updates.CustomerName and silver.Item = updates.Item'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "SalesOrderNumber": "updates.SalesOrderNumber",
      "SalesOrderLineNumber": "updates.SalesOrderLineNumber",
      "OrderDate": "updates.OrderDate",
      "CustomerName": "updates.CustomerName",
      "Email": "updates.Email",
      "Item": "updates.Item",
      "Quantity": "updates.Quantity",
      "UnitPrice": "updates.UnitPrice",
      "Tax": "updates.Tax",
      "FileName": "updates.FileName",
      "IsFlagged": "updates.IsFlagged",
      "CreatedTS": "updates.CreatedTS",
      "ModifiedTS": "updates.ModifiedTS"
    }
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lab03_medallion.sales_silver LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
