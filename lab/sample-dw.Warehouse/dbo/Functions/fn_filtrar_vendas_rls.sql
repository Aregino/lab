-- Agora, recrie a função com a lógica hierárquica
CREATE FUNCTION dbo.fn_filtrar_vendas_rls (
    @cod_empresa VARCHAR(50),
    @cod_canal   VARCHAR(50),
    @cod_setor   VARCHAR(50)
)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS permitido
    FROM dbo.permissao_vendas_rls
    WHERE usuario = USER_NAME()
      AND cod_empresa = @cod_empresa
      AND (
            (cod_canal = @cod_canal AND cod_setor = @cod_setor)
         OR (cod_canal IS NULL AND cod_setor IS NULL)
         OR (cod_canal = @cod_canal AND cod_setor IS NULL)
         OR (cod_canal IS NULL AND cod_setor = @cod_setor)
      );