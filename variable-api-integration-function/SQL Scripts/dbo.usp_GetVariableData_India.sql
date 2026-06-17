SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[usp_GetVariableData]
(
    @LastTimestamp DATETIME = NULL
)
AS
BEGIN

SET NOCOUNT ON;
SET XACT_ABORT ON;

    DECLARE @TimestampStr NVARCHAR(50),
			@COLUMNNAME VARCHAR(MAX),
			@AttributeColumn VARCHAR(2000),
			@SQLSTRING VARCHAR(MAX)

    IF @LastTimestamp IS NULL
        SET @TimestampStr = CONVERT(VARCHAR, DATEADD(DAY, -90, GETDATE()), 120);
    ELSE
        SET @TimestampStr = CONVERT(VARCHAR, @LastTimestamp, 121);

	IF OBJECT_ID('tempdb..#VIEWEMPMST') IS NOT NULL DROP TABLE #VIEWEMPMST

	CREATE TABLE #VIEWEMPMST (EMPCODE VARCHAR(200))

	SELECT @COLUMNNAME =
	(
		SELECT ' ALTER TABLE #VIEWEMPMST ADD ['+ITEMS+'_Description] VARCHAR(2000)'
		FROM   ATTRIBUTE.TYPECONFIGURATION (NOLOCK) t
		CROSS  APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc,'|')) b
		WHERE  Applicable     = 1
		  AND  ModuleCode     = 'CompensationAndBenefits'
		  AND  SubModuleCode  = 'CompensationAndBenefits'
		FOR XML PATH('')
	)

	EXEC(@COLUMNNAME)

	SELECT @AttributeColumn =
	(
		SELECT '['+REPLACE(ITEMS,' ','')+'_Description],'
		FROM   ATTRIBUTE.TYPECONFIGURATION (NOLOCK) t
		CROSS  APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc,'|')) b
		WHERE  Applicable     = 1
		  AND  ModuleCode     = 'CompensationAndBenefits'
		  AND  SubModuleCode  = 'CompensationAndBenefits'
		FOR XML PATH('')
	)

	SET @AttributeColumn = SUBSTRING(@AttributeColumn, 0, LEN(@AttributeColumn))

	IF ISNULL(@AttributeColumn, '') <> ''
	BEGIN
		SET @SQLSTRING =
		'SELECT EmployeeCode,' + @AttributeColumn + '
		 FROM
		 (
			 SELECT EmployeeCode,
					REPLACE(AT.AttributeTypeDescription,'' '','''')+''_Description'' AS AttributeTypeDescription,
					ATU.AttributeTypeUnitDescription
			 FROM   dbo.EmployeeAttributeDetails (NOLOCK) EMP
			 INNER  JOIN AttributeTypeMaster (NOLOCK) AT
				 ON AT.AttributeTypeID = EMP.AttributeTypeID
			 INNER  JOIN AttributeTypeUnitMaster (NOLOCK) ATU
				 ON  ATU.AttributeTypeUnitID = EMP.AttributeTypeUnitID
				 AND EMP.AttributeTypeID     = ATU.AttributeTypeID
			 WHERE  AT.Applicable = ATU.Applicable
			   AND  AT.Applicable = 1
			   AND  (EMP.ToDate IS NULL OR EMP.ToDate = ''1900-01-01 00:00:00.000'')
		 ) AS S
		 PIVOT
		 (
			 MAX(AttributeTypeUnitDescription)
			 FOR AttributeTypeDescription IN (' + @AttributeColumn + ')
		 ) AS PIV'

		INSERT INTO #VIEWEMPMST
		EXEC(@SQLSTRING)
	END

	DECLARE @OrgTbl VARCHAR(100)
	SET @OrgTbl = '##Orgtbl'

	DECLARE @AttributeColumnTax VARCHAR(MAX)

	IF OBJECT_ID('tempdb..#AttributeTable') IS NOT NULL DROP TABLE #AttributeTable

	SELECT	ITEMS
			,AttributeTypeCode
			,AttributeTypeDescription
	INTO #AttributeTable
	FROM Attribute.TypeConfiguration (NOLOCK) A
	CROSS APPLY (SELECT Items FROM dbo.SplitByComma(AttributeBasedOn,'|')) B
	INNER JOIN DBO.AttributeTypeMaster (NOLOCK) AT ON B.ITEMS = AT.AttributeTypeID AND AT.Applicable = 1
	WHERE ModuleCode    = 'CompensationAndBenefits'
	AND SubModuleCode = 'CompensationAndBenefits'
	AND A.Applicable  = 1
	ORDER BY AttributeTypeID

	IF OBJECT_ID('tempdb..#AttributeUnitTable') IS NOT NULL DROP TABLE #AttributeUnitTable

	SELECT	CombinationID
			,PayGroupID
			,Items
			,AttributeTypeID
			,AttributeTypeUnitCode
			,AttributeTypeUnitDescription
	INTO #AttributeUnitTable
	FROM ATTRIBUTE.UNITTYPECONFIGURATION (NOLOCK) A
	INNER JOIN Common.PayGroupMaster (NOLOCK) P ON A.CombinationID = P.ComboID AND A.Applicable = P.Applicable
	CROSS APPLY (SELECT Items FROM dbo.SplitByComma(AttributeUnitConfig,'|')) B
	INNER JOIN DBO.AttributeTypeUnitMaster (NOLOCK) AUT ON B.Items = AUT.AttributeTypeUnitID AND AUT.Applicable = 1
	WHERE A.ModuleCode = 'CompensationAndBenefits'
	AND A.SubModuleCode = 'CompensationAndBenefits'
	AND A.Applicable = 1
	ORDER BY 3

	SELECT @AttributeColumnTax =
	(
		SELECT '['+AttributeTypeDescription+'],'
		FROM   #AttributeTable
		FOR XML PATH('')
	)

	SET @AttributeColumnTax = SUBSTRING(@AttributeColumnTax, 0, LEN(@AttributeColumnTax))

	IF @AttributeColumnTax IS NOT NULL
	BEGIN
		SET @SQLSTRING =
		'SELECT CombinationID, PayGroupID,' + @AttributeColumnTax + '
		 INTO   [' + @OrgTbl + ']
		 FROM
		 (
			 SELECT B.CombinationID, PayGroupID,
					AttributeTypeDescription, AttributeTypeUnitDescription
			 FROM   #AttributeTable  A
			 INNER  JOIN #AttributeUnitTable B ON A.Items = B.AttributeTypeID
			 WHERE  1=1
		 ) AS D
		 PIVOT
		 (
			 MAX(AttributeTypeUnitDescription)
			 FOR AttributeTypeDescription IN (' + @AttributeColumnTax + ')
		 ) AS P'

		EXEC(@SQLSTRING)
	END
	ELSE
	BEGIN
		SELECT 'Paygroup is not Configured'
	END


	DECLARE @Org1 VARCHAR(MAX)
	SET @Org1 = ''
	SET @Org1 =
	(
		SELECT ' AND (A.['+ITEMS+']=B.['+ITEMS+'_Description])'
		FROM   ATTRIBUTE.TYPECONFIGURATION (NOLOCK) t
		CROSS  APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc,'|')) b
		WHERE  Applicable     = 1
		  AND  ModuleCode     = 'CompensationAndBenefits'
		  AND  SubModuleCode  = 'CompensationAndBenefits'
		FOR XML PATH('')
	)

	DROP TABLE IF EXISTS ##Temp2

	SET @SQLSTRING =' SELECT B.Empcode, A.*
					  INTO ##Temp2
					  FROM [' + @OrgTbl + '] A
					  INNER JOIN #VIEWEMPMST B
					  ON 1=1 ' + @Org1

	EXEC(@SQLSTRING)

	SELECT  ED_EMPID EmployeeID
			,ISNULL(ED_EMPCode,'') EmployeeCode
			,ED_CountryID CountryId
			,P.PayHeadCode
			,P.PayHeadID
			,EV.UpdatedDate AS ApprovedDate
			,(CASE WHEN P.PayHeadCategoryID = 2 THEN EV.Amount * -1 ELSE EV.Amount END) AS Amount
			,P.PayHeadCategoryID AS [Type]
			,ISNULL(EFD_PFApplicable,0) AS PFApplicable
			,ISNULL(EFD_PTApplicable,0) PTApplicable
			,ISNULL(EFD_LwfApplicable,0) AS LWFApplicable
			,ISNULL(EFD_ESICApplicable,0) AS ESICApplicable
			,ISNULL(EFD.EFD_VPFApplicable, 0) AS VPFApplicable
			,ISNULL(APG.TaxApp,0) AS TaxApplicable
			,ISNULL(APG.SpotTaxApp,0) AS SpotTaxApplicable
			,ISNULL(APG.OTApplicable,0) AS OTApplicable
	FROM ##Temp2 A
	INNER JOIN PAYROLL.empvariable (NOLOCK) EV ON A.Empcode = EV.Empcode
	INNER JOIN dbo.PayheadMaster (NOLOCK) P ON EV.Headcode = P.PayHeadcode AND P.Applicable = 1
	INNER JOIN Common.AttributePayGroupPayheadDetails (NOLOCK) APG ON A.PaygroupID  = APG.PayGroupID AND P.PayHeadID   = APG.PayHeadID
	INNER JOIN dbo.ReqRec_EmployeeDetails (NOLOCK) ED ON EV.EmpCode = ED.ED_EMPCode
	INNER JOIN dbo.ReqRec_EmployeeFinDetails (NOLOCK) EFD ON ED.ED_EMPID = EFD.EFD_ED_EMPID
	WHERE EV.updatedDate IS NOT NULL 
	AND EV.Amount <> 0
	AND EV.UpdatedDate >= @TimestampStr
	ORDER  BY ED.ED_EMPID

	DROP TABLE IF EXISTS ##Temp2

	IF OBJECT_ID('tempdb..' + @OrgTbl) IS NOT NULL
		EXEC('DROP TABLE ' + @OrgTbl)

END