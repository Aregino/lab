# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "warehouse": {
# META       "default_warehouse": "05a2f385-4b45-495e-9e46-e636a1c9e616",
# META       "known_warehouses": [
# META         {
# META           "id": "05a2f385-4b45-495e-9e46-e636a1c9e616",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

table_name = 'teste'
file_array =  [{"src_file":"sales.csv","dst_table":"sales_1"},{"src_file":"sales2.csv","dst_table":"sales_2"},{"src_file":"sales3.csv","dst_table":"sales_3"}]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prin('Hello world')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
