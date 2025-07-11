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
# META       "default_lakehouse_workspace_id": "cfa78f6c-587c-4b90-8604-31ae9450a998"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Transformar dados para a camada ouro
# 
# 
# Você extraiu com êxito os dados da camada bronze, transformou-os e carregou-os em uma tabela Delta silver. Agora você usará um novo notebook para transformar ainda mais os dados, modelá-los em um esquema em estrela e carregá-los em tabelas Delta gold.
# 
# Observe que você poderia ter feito tudo isso em um único notebook, mas para os fins deste exercício você está usando notebooks separados para demonstrar o processo de transformação de dados de bronze para silver e, em seguida, de silver para ouro. Isso pode ajudar na depuração, solução de problemas e reutilização.
# 
# Retorne à página inicial da Engenharia de Dados e crie um novo notebook chamado Transformar dados para Ouro.
# 
# No painel do Lakehouse Explorer, adicione seu lakehouse de vendas selecionando Adicionar e, em seguida, selecionando o lakehouse de vendas que você criou anteriormente. Você deverá ver a tabela sales_silver listada na seção Tabelas do painel explorer.
# 
# No bloco de código existente, remova o texto padrão e adicione o seguinte código para carregar dados em seu dataframe e começar a criar seu esquema estrela e, em seguida, execute-o:


# CELL ********************

# Load data to the dataframe as a starting point to create the gold layer
df = spark.read.table("lab03_medallion.sales_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adicione um novo bloco de código e cole o código a seguir para criar sua tabela de dimensões de data e executá-la:
from pyspark.sql.types import *
from delta.tables import*
    
# Define the schema for the dimdate_gold table
DeltaTable.createIfNotExists(spark) \
    .tableName("lab03_medallion.dimdate_gold") \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Day", IntegerType()) \
    .addColumn("Month", IntegerType()) \
    .addColumn("Year", IntegerType()) \
    .addColumn("mmmyyyy", StringType()) \
    .addColumn("yyyymm", StringType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Em um novo bloco de código, adicione e execute o seguinte código para criar um dataframe para sua dimensão de data, dimdate_gold:

from pyspark.sql.functions import col, dayofmonth, month, year, date_format
    
# Create dataframe for dimDate_gold

dfdimDate_gold = df.dropDuplicates(["OrderDate"]).select(col("OrderDate"), \
        dayofmonth("OrderDate").alias("Day"), \
        month("OrderDate").alias("Month"), \
        year("OrderDate").alias("Year"), \
        date_format(col("OrderDate"), "MMM-yyyy").alias("mmmyyyy"), \
        date_format(col("OrderDate"), "yyyyMM").alias("yyyymm"), \
    ).orderBy("OrderDate")

# Display the first 10 rows of the dataframe to preview your data

display(dfdimDate_gold.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Você está separando o código em novos blocos de código para que possa entender e observar o que está acontecendo no notebook à medida que os dados são transformados. Em outro novo bloco de código, adicione e execute o seguinte código para atualizar a dimensão de data à medida que novos dados forem recebidos:

# CELL ********************

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, 'Tables/dimdate_gold')

dfUpdates = dfdimDate_gold

deltaTable.alias('silver') \
.merge(
    dfUpdates.alias('updates'),
    'silver.OrderDate = updates.OrderDate'
) \
.whenMatchedUpdate(set =
    {
        
    }
) \
.whenNotMatchedInsert(values =
    {
    "OrderDate": "updates.OrderDate",
    "Day": "updates.Day",
    "Month": "updates.Month",
    "Year": "updates.Year",
    "mmmyyyy": "updates.mmmyyyy",
    "yyyymm": "yyyymm"
    }
) \
.execute()

# Parabéns! Sua dimensão de dados está configurada. Agora você criará sua dimensão de cliente.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Para criar a tabela de dimensões do cliente, adicione um novo bloco de código, cole e execute o código a seguir:

from pyspark.sql.types import *
from delta.tables import *

# Create customer_gold dimension delta table
DeltaTable.createIfNotExists(spark) \
    .tableName("lab03_medallion.dimcustomer_gold") \
    .addColumn("CustomerName", StringType()) \
    .addColumn("Email",  StringType()) \
    .addColumn("First", StringType()) \
    .addColumn("Last", StringType()) \
    .addColumn("CustomerID", LongType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Em um novo bloco de código, adicione e execute o seguinte código para remover clientes duplicados, selecionar colunas específicas e dividir a coluna “CustomerName” para criar as colunas “Primeiro” e “Último” nome:

# CELL ********************

from pyspark.sql.functions import col, split

# Create customer_silver dataframe

dfdimCustomer_silver = df.dropDuplicates(["CustomerName","Email"]).select(col("CustomerName"),col("Email")) \
    .withColumn("First",split(col("CustomerName"), " ").getItem(0)) \
    .withColumn("Last",split(col("CustomerName"), " ").getItem(1)) 

# Display the first 10 rows of the dataframe to preview your data

display(dfdimCustomer_silver.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Aqui, você criou um novo DataFrame dfdimCustomer_silver executando várias transformações, como descartar duplicatas, selecionar colunas específicas e dividir a coluna “CustomerName” para criar colunas de nome “Primeiro” e “Último”. O resultado é um DataFrame com dados de cliente limpos e estruturados, incluindo colunas de nome “First” e “Last” separadas extraídas da coluna “CustomerName”.

# CELL ********************

 # Em seguida, criaremos a coluna ID para nossos clientes. Em um novo bloco de código, cole e execute o seguinte:
from pyspark.sql.functions import monotonically_increasing_id, col, when, coalesce, max, lit
    
dfdimCustomer_temp = spark.read.table("lab03_medallion.dimCustomer_gold")

MAXCustomerID = dfdimCustomer_temp.select(coalesce(max(col("CustomerID")),lit(0)).alias("MAXCustomerID")).first()[0]

dfdimCustomer_gold = dfdimCustomer_silver.join(dfdimCustomer_temp,(dfdimCustomer_silver.CustomerName == dfdimCustomer_temp.CustomerName) & (dfdimCustomer_silver.Email == dfdimCustomer_temp.Email), "left_anti")

dfdimCustomer_gold = dfdimCustomer_gold.withColumn("CustomerID",monotonically_increasing_id() + MAXCustomerID + 1)

# Display the first 10 rows of the dataframe to preview your data

#display(dfdimCustomer_gold.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Aqui você está limpando e transformando dados do cliente (dfdimCustomer_silver) executando uma antijunção esquerda para excluir duplicatas que já existem na tabela dimCustomer_gold e, em seguida, gerando valores customerID exclusivos usando a função monotonically_increasing_id().

# CELL ********************

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, 'Tables/dimcustomer_gold')
    
dfUpdates = dfdimCustomer_gold
    
deltaTable.alias('silver') \
  .merge(
    dfUpdates.alias('updates'),
    'silver.CustomerName = updates.CustomerName AND silver.Email = updates.Email'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "CustomerName": "updates.CustomerName",
      "Email": "updates.Email",
      "First": "updates.First",
      "Last": "updates.Last",
      "CustomerID": "updates.CustomerID"
    }
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Agora você repetirá essas etapas para criar sua dimensão de produto. Em um novo bloco de código, cole e execute o seguinte:

# CELL ********************

from pyspark.sql.types import *
from delta.tables import *
    
DeltaTable.createIfNotExists(spark) \
    .tableName("lab03_medallion.dimproduct_gold") \
    .addColumn("ItemName", StringType()) \
    .addColumn("ItemID", LongType()) \
    .addColumn("ItemInfo", StringType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adicionar outro bloco de código para criar o dataframe product_silver
from pyspark.sql.functions import col, split, lit
    
# Create product_silver dataframe
    
dfdimProduct_silver = df.dropDuplicates(["Item"]).select(col("Item")) \
    .withColumn("ItemName",split(col("Item"), ", ").getItem(0)) \
    .withColumn("ItemInfo",when((split(col("Item"), ", ").getItem(1).isNull() | (split(col("Item"), ", ").getItem(1)=="")),lit("")).otherwise(split(col("Item"), ", ").getItem(1))) 
    
# Display the first 10 rows of the dataframe to preview your data

display(dfdimProduct_silver.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adicionar outro bloco de código para criar o dataframe product_silver.
from pyspark.sql.functions import col, split, lit
    
# Create product_silver dataframe
    
dfdimProduct_silver = df.dropDuplicates(["Item"]).select(col("Item")) \
    .withColumn("ItemName",split(col("Item"), ", ").getItem(0)) \
    .withColumn("ItemInfo",when((split(col("Item"), ", ").getItem(1).isNull() | (split(col("Item"), ", ").getItem(1)=="")),lit("")).otherwise(split(col("Item"), ", ").getItem(1))) 
    
# Display the first 10 rows of the dataframe to preview your data

display(dfdimProduct_silver.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Agora você criará IDs para sua tabela dimProduct_gold. Adicione a sintaxe a seguir em um novo bloco de código e execute-a:
from pyspark.sql.functions import monotonically_increasing_id, col, lit, max, coalesce
    
#dfdimProduct_temp = dfdimProduct_silver
dfdimProduct_temp = spark.read.table("lab03_medallion.dimProduct_gold")
    
MAXProductID = dfdimProduct_temp.select(coalesce(max(col("ItemID")),lit(0)).alias("MAXItemID")).first()[0]
    
dfdimProduct_gold = dfdimProduct_silver.join(dfdimProduct_temp,(dfdimProduct_silver.ItemName == dfdimProduct_temp.ItemName) & (dfdimProduct_silver.ItemInfo == dfdimProduct_temp.ItemInfo), "left_anti")
    
dfdimProduct_gold = dfdimProduct_gold.withColumn("ItemID",monotonically_increasing_id() + MAXProductID + 1)
    
# Display the first 10 rows of the dataframe to preview your data

display(dfdimProduct_gold.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Isso calcula a próxima ID de produto disponível com base nos dados atuais da tabela, atribui essas novas IDs aos produtos e, em seguida, exibe as informações atualizadas do produto.

# MARKDOWN ********************

# **_Semelhante ao que você fez com suas outras dimensões, você precisa garantir que sua tabela de produtos permaneça atualizada à medida que novos dados forem fornecidos. Em um novo bloco de código, cole e execute o seguinte:_**

# CELL ********************

from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/dimproduct_gold')
            
dfUpdates = dfdimProduct_gold
            
deltaTable.alias('silver') \
  .merge(
        dfUpdates.alias('updates'),
        'silver.ItemName = updates.ItemName AND silver.ItemInfo = updates.ItemInfo'
        ) \
        .whenMatchedUpdate(set =
        {
               
        }
        ) \
        .whenNotMatchedInsert(values =
         {
          "ItemName": "updates.ItemName",
          "ItemInfo": "updates.ItemInfo",
          "ItemID": "updates.ItemID"
          }
          ) \
          .execute()

# Agora que você criou suas dimensões, a etapa final é criar a tabela de fatos.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from delta.tables import *
    
DeltaTable.createIfNotExists(spark) \
    .tableName("lab03_medallion.factsales_gold") \
    .addColumn("CustomerID", LongType()) \
    .addColumn("ItemID", LongType()) \
    .addColumn("OrderDate", DateType()) \
    .addColumn("Quantity", IntegerType()) \
    .addColumn("UnitPrice", FloatType()) \
    .addColumn("Tax", FloatType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Em um novo bloco de código, cole e execute o seguinte código para criar um novo dataframe para combinar dados de vendas com informações de clientes e produtos, incluindo ID do cliente, ID do item, data do pedido, quantidade, preço unitário e imposto:

# CELL ********************

from pyspark.sql.functions import col
    
dfdimCustomer_temp = spark.read.table("lab03_medallion.dimCustomer_gold")
dfdimProduct_temp = spark.read.table("lab03_medallion.dimProduct_gold")
    
df = df.withColumn("ItemName",split(col("Item"), ", ").getItem(0)) \
    .withColumn("ItemInfo",when((split(col("Item"), ", ").getItem(1).isNull() | (split(col("Item"), ", ").getItem(1)=="")),lit("")).otherwise(split(col("Item"), ", ").getItem(1))) \
    
    
# Create Sales_gold dataframe
    
dffactSales_gold = df.alias("df1").join(dfdimCustomer_temp.alias("df2"),(df.CustomerName == dfdimCustomer_temp.CustomerName) & (df.Email == dfdimCustomer_temp.Email), "left") \
        .join(dfdimProduct_temp.alias("df3"),(df.ItemName == dfdimProduct_temp.ItemName) & (df.ItemInfo == dfdimProduct_temp.ItemInfo), "left") \
    .select(col("df2.CustomerID") \
        , col("df3.ItemID") \
        , col("df1.OrderDate") \
        , col("df1.Quantity") \
        , col("df1.UnitPrice") \
        , col("df1.Tax") \
    ).orderBy(col("df1.OrderDate"), col("df2.CustomerID"), col("df3.ItemID"))
    
# Display the first 10 rows of the dataframe to preview your data
    
display(dffactSales_gold.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Agora, você garantirá que os dados de vendas permaneçam atualizados executando o seguinte código em um novo bloco de código:

 
from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/factsales_gold')
    
dfUpdates = dffactSales_gold
    
deltaTable.alias('silver') \
  .merge(
    dfUpdates.alias('updates'),
    'silver.OrderDate = updates.OrderDate AND silver.CustomerID = updates.CustomerID AND silver.ItemID = updates.ItemID'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "CustomerID": "updates.CustomerID",
      "ItemID": "updates.ItemID",
      "OrderDate": "updates.OrderDate",
      "Quantity": "updates.Quantity",
      "UnitPrice": "updates.UnitPrice",
      "Tax": "updates.Tax"
    }
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Aqui você está usando a operação de mesclagem do Delta Lake para sincronizar e atualizar a tabela factsales_gold com novos dados de vendas (dffactSales_gold). A operação compara a data do pedido, a ID do cliente e a ID do item entre os dados existentes (tabela silver) e os novos dados (atualiza o DataFrame), atualizando registros correspondentes e inserindo novos registros conforme necessário.
# 
# Agora você tem uma camada ouro modelada e com curadoria que pode ser utilizada para relatar e analisar.

# MARKDOWN ********************

# 
# ### Criar um modelo semântico
# 
# No workspace, agora você pode usar a camada gold para criar um relatório e analisar os dados. Você pode acessar o modelo semântico diretamente no seu espaço de trabalho para criar relacionamentos e medidas para relatórios.
# 
# Observe que não é possível usar o modelo semântico padrão que é criado automaticamente quando você cria um lakehouse. Você deve criar um novo modelo semântico que inclua as tabelas douradas que você criou neste exercício, a partir do gerenciador do lakehouse.
# 
# Em seu workspace, navegue até o lakehouse de vendas.
# Selecione Novo modelo semântico na faixa de opções do modo de exibição do gerenciador do lakehouse.
# Atribua o nome Sales_Gold ao seu novo modelo semântico.
# Selecione suas tabelas de ouro transformadas para incluir no seu modelo semântico e selecione Confirmar.
# dimdate_gold
# dimcustomer_gold
# dimproduct_gold
# factsales_gold
# Isso abrirá o modelo semântico no Fabric, no qual você poderá criar relacionamentos e medidas, conforme mostrado aqui:
# 
# ![teste](https://microsoftlearning.github.io/mslearn-fabric.pt-br/Instructions/Labs/Images/dataset-relationships.png)


# MARKDOWN ********************

# A partir daqui, você ou outros membros da sua equipe de dados podem criar relatórios e dashboards com base nos dados em seu lakehouse. Esses relatórios serão conectados diretamente à camada gold do seu lakehouse, para que eles sempre reflitam os dados mais recentes.
