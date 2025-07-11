# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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

# The Kusto cluster uri to write the data. The query Uri is of the form [https://%3c%3e.kusto.data.microsoft.com]https://<>.kusto.data.microsoft.com 
kustoUri = ""
# The database to write the data
database = ""
# The table to write the data 
table    = ""
# The access credentials for the write
accessToken = notebookutils.credentials.getToken(kustoUri)

# Generate a range of 5 rows with Id's 5 to 9
data = spark.range(5,10) 

# Write data to a Kusto table
data.write.\
    format("com.microsoft.kusto.spark.synapse.datasource").\
    option("kustoCluster",kustoUri).\
    option("kustoDatabase",database).\
    option("kustoTable", table).\
    option("accessToken", accessToken ).\
    option("tableCreateOptions", "CreateIfNotExist").mode("Append").save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#This is an example of Reading data from Kusto. Replace T with your <tablename>
kustoQuery = "T | take 10"
# The Kusto cluster uri to read the data. The query Uri is of the form [https://%3c%3e.kusto.data.microsoft.com]https://<>.kusto.data.microsoft.com 
kustoUri = ""
# The database to read the data
database = ""
# The access credentials for the write
accessToken = notebookutils.credentials.getToken(kustoUri)
kustoDf  = spark.read\
            .format("com.microsoft.kusto.spark.synapse.datasource")\
            .option("accessToken", accessToken)\
            .option("kustoCluster", kustoUri)\
            .option("kustoDatabase", database) \
            .option("kustoQuery", kustoQuery).load()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Note: NotebookUtils is only supported on runtime v1.2 and above. If you are using runtime v1.1, please use mssparkutils instead.
notebookutils.fs.cp(
"file:///synfs/nb_resource/builtin/yourFile",        # Copies the file (or folder) from Notebook resources.
"abfss://<lakehouse ABFS path>",       # Target Lakehouse ABFS path
True        # Recursive?
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Note: NotebookUtils is only supported on runtime v1.2 and above. If you are using runtime v1.1, please use mssparkutils instead.
# create a new Lakehouse in the current workspace
notebookutils.lakehouse.create(name, description="")
# create a new Lakehouse in another workspace
notebookutils.lakehouse.create(name, description="", workspaceId={workspaceId})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# update properties of a Lakehouse in the current workspace
notebookutils.lakehouse.update(name, newName, description="")
# update properties of a Lakehouse in another workspace
notebookutils.lakehouse.update(name, newName, description="", workspaceId={workspaceId})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Lakehouse.table_name LIMIT 1000")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Note: NotebookUtils is only supported on runtime v1.2 and above. If you are using runtime v1.1, please use mssparkutils instead.
DAG = { 
    "activities": [
        { 
            "name": "NotebookSample", # activity name, must be unique 
            "path": "NotebookSample", # notebook path 
            "timeoutPerCellInSeconds": 90, # max timeout for each cell, default to 90 seconds 
            "args": {"p1": "changed value", "p2": 666}, # notebook parameters 
        }, 
        { 
            "name": "NotebookSimple2", 
            "path": "NotebookSimple2", 
            "timeoutPerCellInSeconds": 120, 
            "args": {"p1": "changed value 2", "p2": 777} 
        }, 
        { 
            "name": "NotebookSample2.2", 
            "path": "NotebookSample2", 
            "timeoutPerCellInSeconds": 120, 
            "args": {"p1": "changed value 3", "p2": 888}, 
            "retry": 1, 
            "retryIntervalInSeconds": 10, 
            "dependencies": ["NotebookSample"] # list of activity names that this activity depends on 
        } 
    
    ] 
} 

notebookutils.notebook.runMultiple(DAG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get a notebook in the current workspace
notebookutils.notebook.get(name)
# Get a notebook in another workspace
notebookutils.notebook.get(name, workspaceId={workspaceId})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Note: NotebookUtils is only supported on runtime v1.2 and above. If you are using runtime v1.1, please use mssparkutils instead.
# Reference a notebook and returns its exit value. You can run nesting function calls in a notebook interactively or in a pipeline.
notebookutils.notebook.run("NotebookName")
# Set timeout of each cell to 200s and add a argument for child notebook.
# notebookutils.notebook.run("NotebookName", 200, {"input": 20})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
