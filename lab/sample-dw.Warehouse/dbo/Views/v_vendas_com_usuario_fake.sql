-- Auto Generated (Do not modify) 45D5838FC537635289BAF2B8FA1DE89D971469A80B8FCC62339E15E5DE008253
CREATE   VIEW dbo.v_vendas_com_usuario_fake
AS
SELECT v.*, 
       CAST(NULL AS nvarchar(100)) AS usuario_fake  -- placeholder
FROM dbo.vendas v;