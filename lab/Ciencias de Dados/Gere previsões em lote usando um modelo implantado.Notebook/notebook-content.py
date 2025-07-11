# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "de33c7fd-c0a5-47ea-837f-9f8efd452545",
# META       "default_lakehouse_name": "lab01",
# META       "default_lakehouse_workspace_id": "cfa78f6c-587c-4b90-8604-31ae9450a998"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gere previsões em lote usando um modelo implantado
# 
# Como um cientista de dados, boa parte do seus tempo é dedica ao treinamento modelos de machine learning para identificar padrões complexos em seus dados. Após o treinamento, o objetivo é usar os modelos para recuperar insights valiosos.
# 
# Por exemplo, depois de treinar um modelo com dados históricos de vendas, você pode gerar previsões para a próxima semana. Da mesma forma, usando dados de clientes, é possível treinar um modelo para identificar clientes com maior probabilidade de rotatividade. Seja qual for o caso de uso, após o treinamento do modelo, a intenção é aplicá-lo a um novo conjunto de dados para gerar previsões.
# 
# O Microsoft Fabric auxilia nesse processo, permitindo a aplicação do modelo após acompanhá-lo com o MLflow.
# 
# [Gere Previsões em lote usando um model implantado](https://learn.microsoft.com/pt-br/training/modules/generate-batch-predictions-fabric/1-introduction#:~:text=Gere%20previs%C3%B5es%20em%20lote%20usando%20um%20modelo%20implantado%20no)


# MARKDOWN ********************

# ![ML](https://learn.microsoft.com/pt-br/training/wwl/generate-batch-predictions-fabric/media/data-science-process.png)
# 
# Neste módulo, seu foco será em como gerar previsões em lote. Para obter previsões de um modelo treinado, você precisa salvar o modelo no workspace do Microsoft Fabric. Em seguida, você pode preparar seus novos dados e aplicar o modelo e eles para gerar previsões em lote. Por fim, você pode salvar as previsões no Microsoft Fabric para fazer mais processamentos, como a visualização dos dados em um relatório do Power BI.

# MARKDOWN ********************

# #### Personalizar o comportamento do modelo para pontuações em lote
# 
# Ao treinar um modelo, você deseja usar o modelo para gerar novas previsões. Imagine, por exemplo, que você treinou um modelo de previsão. Toda semana, você aplica o modelo a dados históricos de vendas para gerar a previsão de vendas da próxima semana.
# 
# No Microsoft Fabric, você pode usar um modelo salvo e aplicá-lo aos seus dados para gerar e salvar as novas previsões. O modelo usa os novos dados como entrada, executa as transformações necessárias e gera as previsões.
# 
# As informações sobre as entradas e saídas esperadas do modelo são armazenadas nos artefatos de modelo criados durante o treinamento do modelo. Ao acompanhar seu modelo com o MLflow, você pode modificar o comportamento esperado do modelo durante a pontuação em lote.

# MARKDOWN ********************

# ### Personalizar o comportamento do modelo
# 
# 
# Ao aplicar um modelo treinado a novos dados, é necessário que o modelo compreenda a forma esperada da entrada de dados e como gerar as previsões. As informações sobre entradas e saídas esperadas são armazenadas, juntamente com outros metadados, no arquivo MLmodel.
# 
# Quando você acompanha um modelo de machine learning com MLflow no Microsoft Fabric, as entradas e saídas esperadas do modelo são inferidas. Com o registro automático no MLflow, a pasta model e o arquivo MLmodel são criados automaticamente para você.
# 
# Sempre que você quiser alterar as entradas ou saídas esperadas do modelo, poderá alterar a forma como o arquivo MLmodel é criado quando o modelo é rastreado dentro do workspace do Microsoft Fabric. O esquema da entrada e da saída de dados é definido na assinatura do modelo.

# MARKDOWN ********************

# ### Criar a assinatura do modelo
# 
# Depois de acompanhar um modelo com o MLflow durante o treinamento do modelo, você pode encontrar o arquivo MLmodel na pasta model, armazenada junto com a execução do experimento:
# 
# ![](https://learn.microsoft.com/pt-br/training/wwl/generate-batch-predictions-fabric/media/model-file.png)

# MARKDOWN ********************

# Ao explorar o arquivo MLmodel de exemplo, observe que as entradas e saídas esperadas são definidas como tensores. Quando você aplica o modelo por meio do assistente, apenas uma coluna de entrada é mostrada, pois espera-se que os dados de entrada sejam uma matriz.
# 
# ![](https://learn.microsoft.com/pt-br/training/wwl/generate-batch-predictions-fabric/media/tensor-input.png)

# MARKDOWN ********************

# Para alterar a forma como o modelo deve ser aplicado, você pode definir as várias colunas de entrada e saída esperadas.
# 
# Vamos explorar um exemplo em que você treina um modelo com scikit-learn e usa o registro automático do MLflow para registrar em log todos os outros parâmetros e métricas. Para registrar manualmente um modelo, você pode definir log_models=False.
# 
# Para definir o esquema de entrada, use a classe Schema do MLflow. Você pode especificar as colunas de entrada esperadas, seus tipos de dados e seus respectivos nomes. Da mesma forma, você pode definir o esquema de saída, que geralmente consiste em uma coluna que representa a variável desejada.
# 
# Por fim, você cria o objeto de assinatura de modelo usando a classe ModelSignature do MLflow.

# CELL ********************

from sklearn.tree import DecisionTreeRegressor
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

with mlflow.start_run():
   # Use autologging for all other parameters and metrics
   mlflow.autolog(log_models=False)

   model = DecisionTreeRegressor(max_depth=5)

   # When you fit the model, all other information will be logged 
   model.fit(X_train, y_train)

   # Create the signature manually
   input_schema = Schema([
   ColSpec("integer", "AGE"),
   ColSpec("integer", "SEX"),
   ColSpec("double", "BMI"),
   ColSpec("double", "BP"),
   ColSpec("integer", "S1"),
   ColSpec("double", "S2"),
   ColSpec("double", "S3"),
   ColSpec("double", "S4"),
   ColSpec("double", "S5"),
   ColSpec("integer", "S6"),
   ])

   output_schema = Schema([ColSpec("integer")])

   # Create the signature object
   signature = ModelSignature(inputs=input_schema, outputs=output_schema)

   # Manually log the model
   mlflow.sklearn.log_model(model, "model", signature=signature)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Como resultado, o arquivo MLmodel que está armazenado na pasta de saída model tem a seguinte aparência:
# 
# ![](https://learn.microsoft.com/pt-br/training/wwl/generate-batch-predictions-fabric/media/custom-model.png)

# MARKDOWN ********************

# Ao aplicar o modelo por meio do assistente, você encontrará as colunas de entrada claramente definidas e mais fáceis de alinhar com o conjunto de dados para o qual você deseja gerar previsões.
# 
# ![](https://learn.microsoft.com/pt-br/training/wwl/generate-batch-predictions-fabric/media/custom-wizard.png)
# 


# MARKDOWN ********************

# ### Salvar o modelo no workspace do Microsoft Fabric
# 
# Depois de treinar e acompanhar um modelo de machine learning com o MLflow no Microsoft Fabric, você pode inspecionar o conteúdo da pasta de saída model na execução do experimento. Explorando o arquivo MLmodel especificamente, você pode decidir se o modelo vai se comportar conforme o esperado durante a pontuação em lote.
# 
# Para usar um modelo acompanhado para gerar previsões em lotes, você precisa salvá-lo. Ao salvar um modelo no Microsoft Fabric, você pode:
# 
# - Criar um novo modelo.
# - Adicionar uma nova versão a um modelo existente.
# Para salvar um modelo, você precisa especificar a pasta de saída model, pois essa pasta contém todas as informações necessárias sobre como o modelo deve se comportar durante a pontuação em lote e os próprios artefatos do modelo. Normalmente, o modelo treinado é armazenado como um arquivo pickle na mesma pasta.
# 
# Você pode salvar facilmente um modelo navegando até a execução correspondente do experimento na interface do usuário.
# 
# Como opção alternativa, é possível salvar um modelo por meio do código:


# CELL ********************

# Get the experiment by name
exp = mlflow.get_experiment_by_name(experiment_name)

# List the last experiment run
last_run = mlflow.search_runs(exp.experiment_id, order_by=["start_time DESC"], max_results=1)

# Retrieve the run ID of the last experiment run
last_run_id = last_run.iloc[0]["run_id"]

# Create a path to the model output folder of the last experiment run
model_uri = "runs:/{}/model".format(last_run_id)

# Register or save the model by specifying the model folder and model name
mv = mlflow.register_model(model_uri, "diabetes-model")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Preparar dados antes de gerar previsões
# 
# Ao aplicar um modelo a novos dados, é importante garantir que o esquema dos dados de entrada esteja alinhado com as expectativas do modelo dos dados de entrada.
# 
# De maneira mais específica, você precisa verificar se os tipos de dados das colunas de entrada são os mesmos definidos na assinatura do modelo. Primeiro, vamos obter os dados nos quais você deseja gerar previsões.
# 
# ### Trabalhar com dados em tabelas Delta
# 
# Para aplicar um modelo a novos dados no Microsoft Fabric, você deve armazenar esses novos dados como uma tabela Delta em uma lakehouse.

# MARKDOWN ********************

# ### Entender os tipos de dados na assinatura do modelo
# 
# Ao acompanhar um modelo de machine learning e definir a assinatura no arquivo MLmodel, você tem duas opções para descrever as entradas e saídas esperadas de um modelo. A assinatura de um modelo pode ser:
# 
# - Baseada em coluna – ideal para dados tabulares organizados por colunas.
# - Baseado em tensor – ideal para dados de entrada que você deseja passar como matrizes (imagens, por exemplo).
# 
# No Microsoft Fabric, é comum trabalhar com dados tabulares, por isso é mais frequente usar assinaturas baseadas em colunas. O uso de assinaturas baseadas em coluna facilita o alinhamento das colunas de entrada reais com as colunas de entrada esperadas do modelo.
# 
# Ao definir a assinatura do modelo, você precisa usar tipos de dados do MLflow para especificar o esquema dos dados. Os tipos de dados mais usados são:
# 
# - Boolean: Dados lógicos (True ou False)
# - Datetime: Dados datetime de 64b (por exemplo 2023-10-23 14:30:00).
# - Double: Números de ponto flutuante de 64b (por exemplo 3.14159265359).
# - Float: Números de ponto flutuante de 32b (por exemplo 3.14).
# - Integer: Números inteiros de 32b com sinal (por exemplo 42).
# - Long: Números inteiros de 64b com sinal (por exemplo 1234567890).
# - String: Dados de texto (por exemplo Amsterdam).


# MARKDOWN ********************

# # Definir os tipos de dados dos dados de entrada
# 
# Depois de ingerir os dados em um delta lake e entender a assinatura do modelo, você precisa garantir que os tipos de dados dos dados sejam compatíveis com a entrada esperada pelo modelo.
# 
# Você pode trabalhar com seus dados em um notebook para verificar se os tipos de dados de cada coluna correspondem ao esperado e fazer alterações, se necessário.
# 
# Para listar os tipos de dados de cada coluna de um dataframe df, use o seguinte código:
# 
# **_Idela fazer um código de verificação dos tipos de dados antes da entrada dos dados com o modelo, usando tratativas_** ou alterar quando tem controle:
# 
#     from pyspark.sql.types import IntegerType, DoubleType
# 
#     df = df.withColumn("S1", df["S1"].cast(IntegerType()))
#     df = df.withColumn("S2", df["S2"].cast(DoubleType()))

# MARKDOWN ********************

# # **Gerar e salvar previsões em uma tabela Delta**
# 
# Para gerar previsões, você precisa aplicar um modelo treinado a novos dados. Os dados nos quais você deseja aplicar o modelo devem ser armazenados em uma tabela Delta, e o modelo deve ser salvo no workspace do Microsoft Fabric. Em seguida, você pode usar a função PREDICT para aplicar o modelo aos dados e obter previsões em lote.
# 
# ### _Usar o assistente para gerar a função PREDICT_
# 
# 


# MARKDOWN ********************

# Uma maneira fácil de desenvolver o código para gerar as previsões em lotes é usando o assistente disponível no Microsoft Fabric.
# 
# Após salvar um modelo, você pode navegar até a página do modelo no Microsoft Fabric. Quando você seleciona a opção de **Aplicar esta versão no assistente**, um pop-up é exibido:
# 
# ![](https://learn.microsoft.com/pt-br/training/wwl/generate-batch-predictions-fabric/media/custom-wizard.png)
# 
# Por meio do assistente, você pode selecionar a tabela de entrada, mapear as colunas de entrada para as entradas esperadas pelo modelo e definir a tabela de saída com suas respectivas colunas. Por fim, o código necessário para executar as previsões em lote é gerado para você.

# MARKDOWN ********************

# # **Executar a função PREDICT para aplicar o modelo**
# 
# Você tem a opção de usar o assistente ou criar diretamente o código para gerar previsões. Para aplicar um modelo em um notebook, crie o objeto MLFlowTransformer com os seguintes parâmetros:
# 
# - inputCols: os nomes de colunas do dataframe passados como entradas do modelo.
# - outputCols: o nome(s) da(s) coluna(s) da saída ou das previsões.
# - modelName: o nome do modelo salvo no Microsoft Fabric.
# - modelVersion: a versão do modelo salvo.

# CELL ********************

# Depois de criar o objeto MLFlowTransformer, você pode usá-lo para gerar as previsões em lote no dataframe df executando o código a seguir:

from synapse.ml.predict import MLFlowTransformer

model = MLFlowTransformer(
    inputCols=["AGE","SEX","BMI","BP","S1","S2","S3","S4","S5","S6"],
    outputCol='predictions',
    modelName='diabetes-model',
    modelVersion=1
)

# Depois de criar o objeto MLFlowTransformer, você pode usá-lo para gerar as previsões em lote no dataframe df executando o código a seguir:

model.transform(df).show()

# Por fim, você pode salvar as previsões em uma tabela Delta. É possível escolher entre salvá-las em uma nova tabela ou adicionar as previsões a uma tabela existente. Se você quiser especificar uma tabela existente:

df.write.format("delta").mode("overwrite").save(f"Tables/your_delta_table")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##  **Exercício** 
# 
# [Lab 08d Data Science](https://microsoftlearning.github.io/mslearn-fabric.pt-br/Instructions/Labs/08d-data-science-batch.html)


# CELL ********************

# Insira o código a seguir para carregar e preparar dados e usá-los para treinar um modelo.



import pandas as pd
import mlflow
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

# Get the data
blob_account_name = "azureopendatastorage"
blob_container_name = "mlsamples"
blob_relative_path = "diabetes"
blob_sas_token = r""
wasbs_path = f"wasbs://%s@%s.blob.core.windows.net/%s" % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set("fs.azure.sas.%s.%s.blob.core.windows.net" % (blob_container_name, blob_account_name), blob_sas_token)
df = spark.read.parquet(wasbs_path).toPandas()

# Split the features and label for training
X, y = df[['AGE','SEX','BMI','BP','S1','S2','S3','S4','S5','S6']].values, df['Y'].values
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30, random_state=0)

# Train the model in an MLflow experiment
experiment_name = "experiment-diabetes"
mlflow.set_experiment(experiment_name)
with mlflow.start_run():
    mlflow.autolog(log_models=False)
    model = DecisionTreeRegressor(max_depth=5)
    model.fit(X_train, y_train)
       
    # Define the model signature
    input_schema = Schema([
        ColSpec("integer", "AGE"),
        ColSpec("integer", "SEX"),\
        ColSpec("double", "BMI"),
        ColSpec("double", "BP"),
        ColSpec("integer", "S1"),
        ColSpec("double", "S2"),
        ColSpec("double", "S3"),
        ColSpec("double", "S4"),
        ColSpec("double", "S5"),
        ColSpec("integer", "S6"),
     ])
    output_schema = Schema([ColSpec("integer")])
    signature = ModelSignature(inputs=input_schema, outputs=output_schema)
   
    # Log the model
    mlflow.sklearn.log_model(model, "model", signature=signature)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Use o ícone + Código abaixo da saída da célula para adicionar uma nova célula de código ao notebook e insira o seguinte código para registrar
#  o modelo que foi treinado pelo experimento na célula anterior

# Get the most recent experiement run
exp = mlflow.get_experiment_by_name(experiment_name)
last_run = mlflow.search_runs(exp.experiment_id, order_by=["start_time DESC"], max_results=1)
last_run_id = last_run.iloc[0]["run_id"]

# Register the model that was trained in that run
print("Registering the model from run :", last_run_id)
model_uri = "runs:/{}/model".format(last_run_id)
mv = mlflow.register_model(model_uri, "diabetes-model")
print("Name: {}".format(mv.name))
print("Version: {}".format(mv.version))

''' 
Seu modelo agora está salvo em seu espaço de trabalho como modelo-de-diabetes. Opcionalmente, você pode usar
o recurso de navegação em seu espaço de trabalho para localizar o modelo no espaço de trabalho e explorá-lo usando a interface do usuário.
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Crie um conjunto de dados de teste em um lakehouse**
# 
# 
# Para usar o modelo, você precisará de um conjunto de dados de detalhes de pacientes para os quais você precisa prever um diagnóstico de diabetes. Você criará esse conjunto de dados como uma tabela em um Microsoft Fabric Lakehouse.
# 
# - No editor do Notebook, no painel do Explorer do lado esquerdo, selecione + Fontes de dados para adicionar um lakehouse.
# - Selecione Novo Lakehouse e Adicionar e crie um nov Lakehouse com um nome válido de sua escolha.
# - Quando solicitado a interromper a sessão atual, selecione Parar agora para reiniciar o notebook.
# - Quando o lakehouse for criado e anexado ao notebook, adicione uma nova célula de código e execute o seguinte código para criar um conjunto de dados e salvá-lo em uma tabela no lakehouse:
# 


# CELL ********************

from pyspark.sql.types import IntegerType, DoubleType

# Create a new dataframe with patient data
data = [
    (62, 2, 33.7, 101.0, 157, 93.2, 38.0, 4.0, 4.8598, 87),
    (50, 1, 22.7, 87.0, 183, 103.2, 70.0, 3.0, 3.8918, 69),
    (76, 2, 32.0, 93.0, 156, 93.6, 41.0, 4.0, 4.6728, 85),
    (25, 1, 26.6, 84.0, 198, 131.4, 40.0, 5.0, 4.8903, 89),
    (53, 1, 23.0, 101.0, 192, 125.4, 52.0, 4.0, 4.2905, 80),
    (24, 1, 23.7, 89.0, 139, 64.8, 61.0, 2.0, 4.1897, 68),
    (38, 2, 22.0, 90.0, 160, 99.6, 50.0, 3.0, 3.9512, 82),
    (69, 2, 27.5, 114.0, 255, 185.0, 56.0, 5.0, 4.2485, 92),
    (63, 2, 33.7, 83.0, 179, 119.4, 42.0, 4.0, 4.4773, 94),
    (30, 1, 30.0, 85.0, 180, 93.4, 43.0, 4.0, 5.3845, 88)
]
columns = ['AGE','SEX','BMI','BP','S1','S2','S3','S4','S5','S6']
df = spark.createDataFrame(data, schema=columns)

# Convert data types to match the model input schema
df = df.withColumn("AGE", df["AGE"].cast(IntegerType()))
df = df.withColumn("SEX", df["SEX"].cast(IntegerType()))
df = df.withColumn("BMI", df["BMI"].cast(DoubleType()))
df = df.withColumn("BP", df["BP"].cast(DoubleType()))
df = df.withColumn("S1", df["S1"].cast(IntegerType()))
df = df.withColumn("S2", df["S2"].cast(DoubleType()))
df = df.withColumn("S3", df["S3"].cast(DoubleType()))
df = df.withColumn("S4", df["S4"].cast(DoubleType()))
df = df.withColumn("S5", df["S5"].cast(DoubleType()))
df = df.withColumn("S6", df["S6"].cast(IntegerType()))

# Save the data in a delta table
table_name = "diabetes_test"
df.write.format("delta").mode("overwrite").saveAsTable(table_name)
print(f"Spark dataframe saved to delta table: {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **_Aplicar o modelo para gerar previsões_**


# CELL ********************

# Agora você pode usar o modelo treinado anteriormente para gerar previsões de progressão do diabetes para as linhas de dados do paciente em sua tabela.

import mlflow
from synapse.ml.predict import MLFlowTransformer

## Read the patient features data 
df_test = spark.read.format("delta").load(f"abfss://LAB_UQ@onelake.dfs.fabric.microsoft.com/lab01.Lakehouse/Tables/dbo/{table_name}")

# Use the model to generate diabetes predictions for each row
model = MLFlowTransformer(
    inputCols=["AGE","SEX","BMI","BP","S1","S2","S3","S4","S5","S6"],
    outputCol="predictions",
    modelName="diabetes-model",
    modelVersion=1)
df_test = model.transform(df)

# Save the results (the original features PLUS the prediction)
df_test.write.format('delta').mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lab01.dbo.diabetes_test LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
df = spark.sql("SELECT * FROM lab01.dbo.diabetes_test order by 'AGE' ")

df=df.orderBy(F.col('AGE'))
df = df.toPandas()
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Estatísticas descritivas da coluna 'predictions'
y_stats = df["predictions"].describe()
y_stats



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Criar colunas de classificação de risco

# Critério Percentil 75%
df["risk_percentile"] = (df["predictions"] > 143.0).astype(int)

# Critério Média + Desvio Padrão
df["risk_mean_std"] = (df["predictions"] > (125.1 + 59.8)).astype(int)

# Exibir os primeiros resultados
print(df.head())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Criar duas novas colunas para classificação de risco

# Critério Percentil 75%
df["risk_percentile"] = (df["predictions"] > 143.0).astype(int)

# Critério Média + Desvio Padrão
df["risk_mean_std"] = (df["predictions"] > (125.1 + 59.8)).astype(int)

# Exibir os dados com as novas classificações
df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dados originais
# Azure storage access info for open dataset diabetes
import pyspark.sql.functions as F

blob_account_name = "azureopendatastorage"
blob_container_name = "mlsamples"
blob_relative_path = "diabetes"
blob_sas_token = r"" # Blank since container is Anonymous access
    
# Set Spark config to access  blob storage
wasbs_path = f"wasbs://%s@%s.blob.core.windows.net/%s" % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set("fs.azure.sas.%s.%s.blob.core.windows.net" % (blob_container_name, blob_account_name), blob_sas_token)
print("Remote blob path: " + wasbs_path)
    
# Spark read parquet, note that it won't load any data yet by now
df_original = spark.read.parquet(wasbs_path)

display(df_original.orderBy(F.col('AGE')))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

30,1,30.0,85.0,180,93.4,43.0,4.0,5.3845,88,255,1,1

30,1,21.3,87.0,134,63.0,63.0,2.0,3.6889,66,143

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
