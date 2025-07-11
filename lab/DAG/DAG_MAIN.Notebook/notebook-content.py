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

# CELL ********************

from notebookutils import mssparkutils

DAG = {
    "activities": [
        {
            "name": "DAG_01",  # Nome único da atividade
            "path": "DAG_01",  # Caminho correto do notebook
            "timeoutPerCellInSeconds": 90,  # Timeout de cada célula
            "args": {"name": "parametro_aceito"}  # Parâmetros do notebook
        },
        {
            "name": "DAG_02",
            "path": "DAG_02",
            "timeoutPerCellInSeconds": 120,
            "args": {"name": "parametro_aceito_2"},  # Corrigido erro de sintaxe
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "dependencies": ["DAG_01"]  # Define que DAG_02 só roda depois de DAG_01
        }
    ]
}

# Executando a DAG com mssparkutils
mssparkutils.notebook.runMultiple(DAG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
