-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "519e0c3d-96c5-4267-918b-a0c0d179e73e",
-- META       "default_lakehouse_name": "atividade1",
-- META       "default_lakehouse_workspace_id": "cfa78f6c-587c-4b90-8604-31ae9450a998",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "519e0c3d-96c5-4267-918b-a0c0d179e73e"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "known_warehouses": []
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE TABLE permissao_usuario (
    usuario VARCHAR(100)
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- Primeiro, se a tabela 'vendas' foi criada com erro, você pode querer dropá-la:
-- DROP TABLE vendas;
-- Em seguida, crie-a novamente com a definição correta:

CREATE TABLE vendas (
    id_venda VARCHAR(20), -- id_venda agora é apenas um INT com PRIMARY KEY
    cod_empresa VARCHAR(50),
    cod_canal VARCHAR(50),
    cod_setor VARCHAR(50),
    valor_venda DECIMAL(10, 2),
    data_venda DATE
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE vendas (
    id_venda VARCHAR(20), -- id_venda agora é apenas um INT com PRIMARY KEY
    cod_empresa VARCHAR(50),
    cod_canal VARCHAR(50),
    cod_setor VARCHAR(50),
    valor_venda DECIMAL(10, 2),
    data_venda DATE
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

INSERT INTO vendas (id_venda, cod_empresa, cod_canal, cod_setor, valor_venda, data_venda) VALUES
(1, 'Empresa A', 'Online', 'Eletronicos', 1500.00, '2024-05-01'),
(2, 'Empresa A', 'Loja Fisica', 'Vestuario', 300.50, '2024-05-02'),
(3, 'Empresa B', 'Online', 'Alimentos', 80.75, '2024-05-03'),
(4, 'Empresa B', 'Loja Fisica', 'Brinquedos', 250.00, '2024-05-04'),
(5, 'Empresa C', 'Online', 'Eletronicos', 2200.00, '2024-05-05'),
(6, 'Empresa A', 'Loja Fisica', 'Eletronicos', 700.00, '2024-05-06'),
(7, 'Empresa C', 'Online', 'Vestuario', 120.00, '2024-05-07'),
(8, 'Empresa B', 'Loja Fisica', 'Alimentos', 45.90, '2024-05-08');

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE TABLE permissao_vendas_rls (
    usuario VARCHAR(100),
    cod_empresa VARCHAR(50),
    cod_canal VARCHAR(50),
    cod_setor VARCHAR(50)
);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- Função sem parâmetros
CREATE FUNCTION fn_filtrar_vendas_rls()
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT cod_empresa, cod_canal, cod_setor
    FROM dbo.permissao_vendas_rls
    WHERE usuario = USER_NAME();


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
