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

# PARAMETERS CELL ********************

name = 'dag'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/new_data/sales.csv")
# df now is a Spark DataFrame containing CSV data from "Files/new_data/sales.csv".


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .save(f'Files/new_data/dag/{name}.csv')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
