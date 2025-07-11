# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

df = df.toPandas()
df.head()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_clean.describe()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Train machine learning models
# 
# Now that you’ve prepared the data, you can use it to train a machine learning model to predict diabetes. We can train two different types of models with our dataset: a regression model (predicting Y) or a classification model (predicting Risk). You’ll train the models using the scikit-learn library and track the models with MLflow.


# CELL ********************

# Train a regression model
#Run the following code to split the data into a training 
#and test dataset, and to separate the features from the label Y you want to predict:

from sklearn.model_selection import train_test_split
    
X, y = df_clean[['AGE','SEX','BMI','BP','S1','S2','S3','S4','S5','S6']].values, df_clean['Y'].values
    
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30, random_state=0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add another new code cell to the notebook, enter the following code in it, and run it:
import mlflow


experiment_name = "diabetes-regression"
mlflow.set_experiment(experiment_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add another new code cell to the notebook, enter the following code in it, and run it:

from sklearn.linear_model import LinearRegression
    
with mlflow.start_run():
   mlflow.autolog()
    
   model = LinearRegression()
   model.fit(X_train, y_train)

# The code trains a regression model using Linear Regression. Parameters, metrics,
# and artifacts, are automatically logged with MLflow

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Train a classification model
# Run the following code to split the data into a training and test dataset, and to separate the **features**
# from the label Risk you want to predict:

# CELL ********************

from sklearn.model_selection import train_test_split
    
X, y = df_clean[['AGE','SEX','BMI','BP','S1','S2','S3','S4','S5','S6']].values, df_clean['Risk'].values
    
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30, random_state=0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add another new code cell to the notebook, enter the following code in it, and run it:

import mlflow
experiment_name = "diabetes-classification"
mlflow.set_experiment(experiment_name)

# The code creates an MLflow experiment named diabetes-classification. Your models will be tracked in this experiment.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.linear_model import LogisticRegression
    
with mlflow.start_run():
    mlflow.sklearn.autolog()

    model = LogisticRegression(C=1/0.1, solver="liblinear").fit(X_train, y_train)

# The code trains a classification model using Logistic Regression. Parameters, metrics, and artifacts, are automatically logged with MLflow.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
