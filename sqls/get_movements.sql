DECLARE @UNEM_ID VARCHAR(22)
DECLARE @PROD_ID VARCHAR(22)

DECLARE @DAYS TABLE (DATA DATE)

SET @UNEM_ID = ':id_company'
SET @PROD_ID = ':id_product'

--SET @UNEM_ID = '001410060001'
--SET @PROD_ID = '001410040000000013'

INSERT INTO @DAYS(DATA) VALUES(DATEADD(YEAR,-2,GETDATE()))

WHILE (SELECT MAX(DATA) FROM @DAYS) < GETDATE()
  INSERT INTO @DAYS(DATA) VALUES(DATEADD(DAY, 1, (SELECT MAX(DATA) FROM @DAYS)))
  
SELECT
   DATES.DATA AS 'date', ISNULL(SALES,0) AS 'sales', ISNULL(QUANTITY,0) AS 'quantity'
FROM
   @DAYS DATES
LEFT JOIN
   (SELECT
      CAST(DCFS.DCFS_DATA_SAIDA AS DATE) DATA,
      COUNT(DISTINCT DCFS.DCFS_ID) AS SALES,
      SUM(ITFT.ITFT_QTDE_FATURADA) AS QUANTITY
   FROM
      DOCUMENTOS_FISCAIS DCFS
   LEFT JOIN 
      ITENS_FATURADOS ITFT
      ON DCFS.DCFS_ID = ITFT.DCFS_ID
   WHERE
      ITFT.PROD_ID = @PROD_ID AND
      DCFS.DCFS_ID LIKE @UNEM_ID + '%' AND   
      DCFS.DCFS_STATUS = 'V�lido' AND
      DCFS.DCFS_TIPO_MOVIMENTO = 'Sa�da' AND
      DCFS.DCFS_NATUREZA_OPERACAO = 'Venda'
   GROUP BY
      CAST(DCFS.DCFS_DATA_SAIDA AS DATE)
   ) DCFS
   ON DCFS.DATA = DATES.DATA
ORDER BY
   DATES.DATA