# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "426878d1-ea93-4438-a13e-45d9132acac7",
# META       "default_lakehouse_name": "lab02",
# META       "default_lakehouse_workspace_id": "cfa78f6c-587c-4b90-8604-31ae9450a998"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Sales order data exploration
# Use this notebook to explore sales order data


# CELL ********************

 df = spark.read.format("csv").option("header","true").load("Files/orders/2019.csv")
 # df now is a Spark DataFrame containing CSV data from "Files/orders/2019.csv".
 display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","false").load("Files/orders/2019.csv")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

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

df = spark.read.format("csv").schema(orderSchema).load("Files/orders/2019.csv")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Carrega todos arquivos da pasta

from pyspark.sql.types import *

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

df = spark.read.format("csv").schema(orderSchema).load("Files/orders/*.csv")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers = df['CustomerName', 'Email']

print(customers.count())
print(customers.distinct().count())

display(customers.distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers = df.select("CustomerName", "Email")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers = df.select("CustomerName", "Email").where(df['Item']=='Road-250 Red, 52')
print(customers.count())
print(customers.distinct().count())

display(customers.distinct())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

productSales = df.select("Item", "Quantity").groupBy("Item").sum()

display(productSales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

yearlySales = df.select(year(col("OrderDate")).alias("Year")).groupBy("Year").count().orderBy("Year")

display(yearlySales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''
A instrução import permite que você use a biblioteca SQL do Spark.
O método select é usado com uma função year do SQL para extrair o componente de ano do campo OrderDate.
O método alias para atribuir um nome de coluna ao valor de ano extraído.
O método groupBy agrupa os dados pela coluna Year derivada.
A contagem de linhas em cada grupo é calculada antes de o método orderBy ser usado para classificar o DataFrame resultante.
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ![data](https://microsoftlearning.github.io/mslearn-fabric.pt-br/Instructions/Labs/Images/spark-sql-dataframe.jpg)

# CELL ********************

from pyspark.sql.functions import *

# Create Year and Month columns
transformed_df = df.withColumn("Year", year(col("OrderDate"))).withColumn("Month", month(col("OrderDate")))

# Create the new FirstName and LastName fields
transformed_df = transformed_df.withColumn("FirstName", split(col("CustomerName"), " ").getItem(0)).withColumn("LastName", split(col("CustomerName"), " ").getItem(1))

# Filter and reorder columns
transformed_df = transformed_df["SalesOrderNumber", "SalesOrderLineNumber", "OrderDate", "Year", "Month", "FirstName", "LastName", "Email", "Item", "Quantity", "UnitPrice", "Tax"]

# Display the first five orders
display(transformed_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''
Execute a célula. Um novo DataFrame com base nos dados do pedido original com as seguintes transformações:

Colunas Year e Month adicionadas com base na coluna OrderDate.
Colunas FirstName e LastName adicionadas com base na coluna CustomerName.
As colunas são filtradas e reordenadas, e a coluna CustomerName é removida.
Analise a saída e verifique se as transformações foram feitas nos dados.

Você pode usar a biblioteca do Spark SQL para transformar os dados filtrando linhas, derivando, removendo, renomeando colunas e aplicando outras modificações de dados.
'''


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# - **Salvar os dados transformados**
# 
# A esta altura, talvez você queira salvar os dados transformados para usá-los para análise posterior.
# 
# O Parquet é um formato popular de armazenamento de dados porque armazena dados com eficiência e é compatível com a maioria dos sistemas de análise de dados em grande escala. Na verdade, às vezes, o requisito de transformação de dados é converter dados de um formato, como CSV, em Parquet.
# 
# Para salvar o DataFrame transformado no formato Parquet, adicione uma célula de código e adicione o seguinte código:

# CELL ********************

transformed_df.write.mode("overwrite").parquet('Files/transformed_data/orders')

print ("Transformed data saved!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Execute a célula e aguarde a mensagem indicando que os dados foram salvos. Em seguida, no painel do Lakehouses à esquerda, no menu … do nó Arquivos, clique em Atualizar. Escolha a pasta transformed_data para verificar se ela contém uma nova pasta chamada orders, que por sua vez contém um ou mais arquivos Parquet.

# CELL ********************

orders_df = spark.read.format("parquet").load("Files/transformed_data/orders")
display(orders_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Execute a célula. Um novo DataFrame é criado a partir dos arquivos parquet na pasta transformed_data/orders. Verifique se os resultados mostram os dados do pedido que foram carregados a partir dos arquivos Parquet.
# 
# 
# ![arquivos_Parquet](https://microsoftlearning.github.io/mslearn-fabric.pt-br/Instructions/Labs/Images/parquet-files.jpg)

# MARKDOWN ********************

# ## _Salvar os dados em arquivos particionados_
# 
# Ao lidar com grandes volumes de dados, o particionamento pode melhorar significativamente o desempenho e facilitar a filtragem de dados.

# CELL ********************

orders_df.write.partitionBy("Year","Month").mode("overwrite").parquet("Files/partitioned_data")

print ("Transformed data saved!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''
Execute a célula e aguarde a mensagem indicando que os dados foram salvos. Em seguida,
no painel do Lakehouses à esquerda, no menu … do nó Arquivos, clique em Atualizar e expanda
a pasta partitioned_data para verificar se ela contém uma hierarquia de pastas chamada Year=xxxx,
cada uma contendo pastas chamadas Month=xxxx. Cada pasta mensal contém um arquivo Parquet com os pedidos desse mês.
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ![link-alt-text](https://microsoftlearning.github.io/mslearn-fabric.pt-br/Instructions/Labs/Images/partitioned-data.jpg)

# CELL ********************

# para fazer delta em arquivo grandes se for apra particionar basta fazer leitura do arquivos que sofreão atualizações

orders_2021_df = spark.read.format("parquet").load("Files/partitioned_data/Year=2021/Month=*")

display(orders_2021_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Trabalhar com tabelas e o SQL
# Como você viu, os métodos nativos do objeto DataFrame permitem que você consulte e analise os dados a partir de um arquivo. No entanto, você pode se sentir mais confortável trabalhando com tabelas usando a sintaxe SQL. O Spark fornece um metastore no qual você pode definir tabelas relacionais.
# 
# A biblioteca do Spark SQL permite o uso de instruções SQL para consultar tabelas no metastore. Isso dá a flexibilidade de um data lake com o esquema de dados estruturado e as consultas baseadas em SQL de um data warehouse relacional, daí o termo “data lakehouse”.

# CELL ********************

'''
As tabelas em um metastore do Spark são abstrações relacionais em arquivos no data lake. As tabelas podem ser gerenciadas pelo metastore ou externas e gerenciadas de forma independente do metastore.

Adicione uma célula de código ao notebook e insira o seguinte código, que salva o DataFrame dos dados do pedido de vendas como uma tabela chamada salesorders:
'''


# Create a new table
df.write.format("delta").saveAsTable("salesorders")

# Get the table description
spark.sql("DESCRIBE EXTENDED salesorders").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# [!NOTE] 
# 
# Neste exemplo, nenhum caminho explícito é fornecido; portanto, os arquivos da tabela serão gerenciados pelo metastore. Além disso, a tabela é salva no formato delta, que adiciona recursos de banco de dados relacional às tabelas. Isso inclui suporte para transações, controle de versão de linha e outros recursos úteis. A criação de tabelas no formato delta é preferencial para data lakehouses no Fabric.

# CELL ********************

 # Lendo a tabela criada

df = spark.sql("SELECT * FROM lab02.salesorders LIMIT 1000")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Executar um código SQL em uma célula
# 


# MARKDOWN ********************

# Embora seja útil a inserção de instruções SQL em uma célula que contém código PySpark, os analistas de dados muitas vezes preferem trabalhar diretamente com SQL.

# CELL ********************

 %%sql
SELECT YEAR(OrderDate) AS OrderYear,
       SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue
FROM salesorders
GROUP BY YEAR(OrderDate)
ORDER BY OrderYear;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# - O comando %%sql no início da célula (chamado de magic) altera a linguagem para Spark SQL em vez de PySpark.
# - O código SQL referencia a tabela salesorders que você já criou.
# - A saída da consulta SQL é exibida automaticamente como o resultado abaixo da célula.

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM salesorders

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sqlQuery = "SELECT CAST(YEAR(OrderDate) AS CHAR(4)) AS OrderYear, \
                SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue \
            FROM salesorders \
            GROUP BY CAST(YEAR(OrderDate) AS CHAR(4)) \
            ORDER BY OrderYear"
df_spark = spark.sql(sqlQuery)
df_spark.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Introdução à matplotlib


# CELL ********************

from matplotlib import pyplot as plt

# matplotlib requires a Pandas dataframe, not a Spark one
df_sales = df_spark.toPandas()

# Create a bar plot of revenue by year
plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'])

# Display the plot
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from matplotlib import pyplot as plt

# Clear the plot area
plt.clf()

# Create a bar plot of revenue by year
plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')

# Customize the chart
plt.title('Revenue by Year')
plt.xlabel('Year')
plt.ylabel('Revenue')
plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
plt.xticks(rotation=45)

# Show the figure
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from matplotlib import pyplot as plt

# Clear the plot area
plt.clf()

# Create a Figure
fig = plt.figure(figsize=(8,3))

# Create a bar plot of revenue by year
plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')

# Customize the chart
plt.title('Revenue by Year')
plt.xlabel('Year')
plt.ylabel('Revenue')
plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
plt.xticks(rotation=45)

# Show the figure
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from matplotlib import pyplot as plt

# Clear the plot area
plt.clf()

# Create a figure for 2 subplots (1 row, 2 columns)
fig, ax = plt.subplots(1, 2, figsize = (10,4))

# Create a bar plot of revenue by year on the first axis
ax[0].bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')
ax[0].set_title('Revenue by Year')

# Create a pie chart of yearly order counts on the second axis
yearly_counts = df_sales['OrderYear'].value_counts()
ax[1].pie(yearly_counts)
ax[1].set_title('Orders per Year')
ax[1].legend(yearly_counts.keys().tolist())

# Add a title to the Figure
fig.suptitle('Sales Data')

# Show the figure
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Usar a biblioteca seaborn


# CELL ********************

import seaborn as sns

# Clear the plot area
plt.clf()

# Create a bar chart
ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)

plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 import seaborn as sns

 # Clear the plot area
 plt.clf()

 # Set the visual theme for seaborn
 sns.set_theme(style="whitegrid")

 # Create a bar chart
 ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)

 plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import seaborn as sns

# Clear the plot area
plt.clf()

# Create a line chart
ax = sns.lineplot(x="OrderYear", y="GrossRevenue", data=df_sales)

plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
