# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Treinar e acompanhar modelos de machine learning com o MLflow
# 
# 
# 
# A maneira como você aborda o treinamento de um aprendizado de máquina depende do tipo de modelo que você treina. Uma abordagem comum com modelos tradicionais é a iteração por meio das etapas a seguir:
# 
# - Carregue os dados, disponibilizando-os no notebook como um DataFrame.
# - Explore os dados visualizando-os e compreendendo o relacionamento entre os recursos (entrada do modelo) e como eles afetam o rótulo (entrada do modelo). (entrada do modelo) e como isso afeta o rótulo (saída do modelo).
# - Preparar os dados, também conhecido como engenharia de recursos.
# - Dividir os dados em um conjunto de dados de treinamento e um conjunto de dados de teste.
# - Treinar o modelo.
# - Avalie o modelo inspecionando as métricas de desempenho.
# 
# Vamos explorar um exemplo e supor que você já tenha um conjunto de dados que explorou e preparou para o treinamento do modelo. Imagine que você deseja treinar um modelo de regressão e optou por utilizar o scikit-learn.
# 
# Você pode dividir o conjunto de dados preparado com o código a seguir:


# CELL ********************

# Azure storage access info for open dataset diabetes
blob_account_name = "azureopendatastorage"
blob_container_name = "mlsamples"
blob_relative_path = "diabetes"
blob_sas_token = r"" # Blank since container is Anonymous access
    
# Set Spark config to access  blob storage
wasbs_path = f"wasbs://%s@%s.blob.core.windows.net/%s" % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set("fs.azure.sas.%s.%s.blob.core.windows.net" % (blob_container_name, blob_account_name), blob_sas_token)
print("Remote blob path: " + wasbs_path)
    
# Spark read parquet, note that it won't load any data yet by now
df = spark.read.parquet(wasbs_path)

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

import pandas as pd
df = df.toPandas()
df.head()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Treinar um modelo de machine learning**
# 
# Agora que carregou os dados, você poderá usá-los para treinar um modelo de machine learning e prever uma medida quantitativa do diabetes. Você treinará um modelo de regressão usando a biblioteca do scikit-learn e acompanhará o modelo com o MLflow

# CELL ********************

# Execute o código a seguir para dividir os dados em um conjunto de dados de treinamento e teste e separar os recursos do rótulo que você deseja prever:

from sklearn.model_selection import train_test_split
    
X, y = df[['AGE','SEX','BMI','BP','S1','S2','S3','S4','S5','S6']].values, df['Y'].values

# X representa as variáveis independentes (também chamadas de features), 
# que são usadas como entrada no modelo. No caso, são as colunas 'AGE', 'SEX', 'BMI', 'BP', 'S1', 'S2', 'S3', 'S4', 'S5', 'S6' do DataFrame.
# y representa a variável dependente (também chamada de target ou rótulo), que é o valor que queremos prever. No caso, é a coluna 'Y'.
# x vai conter o dataframe exceto o y e o somente o valor de predição

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30, random_state=0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adicione outra nova célula de código ao notebook e insira o seguinte código nela, executando-a:

import mlflow
experiment_name = "experiment-diabetes"
mlflow.set_experiment(experiment_name)

# O código cria um experimento do MLflow chamado experiment-diabetes. Seus modelos serão rastreados neste experimento.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adicione outra nova célula de código ao notebook e insira o seguinte código nela, executando-a:


from sklearn.linear_model import LinearRegression
    
with mlflow.start_run():
   mlflow.autolog()
    
   model = LinearRegression()
   model.fit(X_train, y_train)
    
   mlflow.log_param("estimator", "LinearRegression")

   '''
   O código treina um modelo de regressão usando Regressão Linear. Os parâmetros, 
   as métricas e os artefatos são registrados em log automaticamente no MLflow. Além disso, 
   você está registrando um parâmetro chamado estimador com o valor LinearRegression.
   '''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adicione outra nova célula de código ao notebook e insira o seguinte código nela, executando-a:

from sklearn.tree import DecisionTreeRegressor
    
with mlflow.start_run():
   mlflow.autolog()
    
   model = DecisionTreeRegressor(max_depth=5) 
   model.fit(X_train, y_train)
    
   mlflow.log_param("estimator", "DecisionTreeRegressor")

   '''
   O código treina um modelo de regressão usando o Regressor de Árvore de Decisão. Os parâmetros, 
   as métricas e os artefatos são registrados em log automaticamente no MLflow. 
   Além disso, você está registrando um parâmetro chamado estimador com o valor DecisionTreeRegressor.
   '''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **Usar o MLflow para pesquisar e ver seus experimentos**
# 
# Ao treinar e acompanhar modelos com o MLflow, você pode usar a biblioteca do MLflow para recuperar seus experimentos e os detalhes.
# 


# CELL ********************

import mlflow
experiments = mlflow.search_experiments()
for exp in experiments:
    print(exp.name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Para recuperar um experimento específico, você pode obtê-lo pelo nome:

experiment_name = "experiment-diabetes"
exp = mlflow.get_experiment_by_name(experiment_name)
print(exp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Usando um nome de experimento, recupere todos os trabalhos desse experimento:

mlflow.search_runs(exp.experiment_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Para comparar com mais facilidade as execuções e as saídas do trabalho, configure a pesquisa para ordenar os resultados. Por exemplo, a célula a seguir ordena os resultados por start_time e mostra apenas um máximo de 2 resultados:
mlflow.search_runs(exp.experiment_id, order_by=["start_time DESC"], max_results=2)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Por fim, você pode plotar as métricas de avaliação de vários modelos um ao lado do outro para comparar os modelos com facilidade:

import matplotlib.pyplot as plt
   
df_results = mlflow.search_runs(exp.experiment_id, order_by=["start_time DESC"], max_results=2)[["metrics.training_r2_score", "params.estimator"]]
   
fig, ax = plt.subplots()
ax.bar(df_results["params.estimator"], df_results["metrics.training_r2_score"])
ax.set_xlabel("Estimator")
ax.set_ylabel("R2 score")
ax.set_title("R2 score by Estimator")
for i, v in enumerate(df_results["metrics.training_r2_score"]):
    ax.text(i, v, str(round(v, 2)), ha='center', va='bottom', fontweight='bold')
plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Explorar seus experimentos**

