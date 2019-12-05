DECLARE @SAZO_ID VARCHAR(22)
      , @UNEM_ID VARCHAR(22)
	  
SET @UNEM_ID = ':id_company'

WHILE 1 = 1
BEGIN
  EXEC dbo.sp_Formata_Id @UNEM_ID,'ID', 'SAZONALIDADES', @SAZO_ID OUTPUT
  SET @SAZO_ID = LTRIM(RTRIM(@SAZO_ID))
  IF NOT EXISTS(SELECT SAZO_ID FROM SAZONALIDADES WHERE SAZO_ID = @SAZO_ID)
    BREAK
END

INSERT INTO
   SAZONALIDADES
   (SAZO_ID, PROD_ID, SAZO_MES_INICIAL, SAZO_DIA_INICIAL,
   SAZO_MES_FINAL, SAZO_DIA_FINAL, SAZO_QTDE_MINIMA,
   SAZO_ATIVO, SAZO_CADASTRO_AUTOMATICO, SAZO_USRS_ID,
   SAZO_LASTUPDATE)
VALUES
   (@SAZO_ID, ':id_product', :begin_month, :begin_date,
   :end_month, :end_day, :quantity, 'Sim',
   'Sim', NULL, GETDATE())
