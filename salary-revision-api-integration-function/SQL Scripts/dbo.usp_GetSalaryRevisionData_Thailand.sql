
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[usp_GetSalaryRevisionData]
(
    @LastTimestamp DATETIME = NULL
)
AS
BEGIN

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @TimestampStr NVARCHAR(50)

IF @LastTimestamp IS NULL
	SET @TimestampStr = CONVERT(VARCHAR, DATEADD(DAY, -90, GETDATE()), 120);
ELSE
	SET @TimestampStr = CONVERT(VARCHAR, @LastTimestamp, 121);

DECLARE	@Effective_start_date DATE = DATEFROMPARTS(YEAR(GETDATE())-1, 1, 1)	--2025-01-01
		,@CountryID INT = (SELECT CountryID FROM ELCM_COMMONCORE.dbo.signupdetails WHERE organizationName = (SELECT REPLACE(DB_NAME(),'ELCM_','')))

DECLARE @COLUMNNAME NVARCHAR(MAX),
        @AttributeColumn NVARCHAR(MAX),
        @SQLSTRING NVARCHAR(MAX);
 
IF OBJECT_ID('tempdb..#VIEWEMPMST') IS NOT NULL 
DROP TABLE #VIEWEMPMST;

CREATE TABLE #VIEWEMPMST
(
    EMPCODE VARCHAR(200)
);
 
SELECT @COLUMNNAME =
(
    SELECT ' ALTER TABLE #VIEWEMPMST ADD [' + ITEMS + '_Description] VARCHAR(2000)'
    FROM ATTRIBUTE.TYPECONFIGURATION (NOLOCK) t
    CROSS APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc,'|')) b
    WHERE Applicable = 1
    AND ModuleCode = 'CompensationAndBenefits'
    AND SubModuleCode = 'CompensationAndBenefits'
    FOR XML PATH('')
);
 
EXEC sp_executesql @COLUMNNAME;
 
SELECT @AttributeColumn =
(
    SELECT '[' + REPLACE(ITEMS,' ','') + '_Description],'
    FROM ATTRIBUTE.TYPECONFIGURATION (NOLOCK) t
    CROSS APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc,'|')) b
    WHERE Applicable = 1
    AND ModuleCode = 'CompensationAndBenefits'
    AND SubModuleCode = 'CompensationAndBenefits'
    FOR XML PATH('')
);
 
SET @AttributeColumn = LEFT(@AttributeColumn, LEN(@AttributeColumn)-1);
 
IF ISNULL(@AttributeColumn,'') <> ''
BEGIN
SET @SQLSTRING='
SELECT EmployeeCode,'+@AttributeColumn+'
FROM
(
SELECT EmployeeCode,REPLACE(AT.AttributeTypeDescription,'' '','''')+''_Description'' AS AttributeTypeDescription,ATU.AttributeTypeUnitDescription
FROM dbo.EmployeeAttributeDetails (NOLOCK) EMP
INNER JOIN AttributeTypeMaster (NOLOCK) AT ON AT.AttributeTypeID = EMP.AttributeTypeID
INNER JOIN AttributeTypeUnitMaster (NOLOCK) ATU ON ATU.AttributeTypeUnitID = EMP.AttributeTypeUnitID AND EMP.AttributeTypeID = ATU.AttributeTypeID
WHERE AT.Applicable = ATU.Applicable
AND AT.Applicable = 1
AND (EMP.ToDate IS NULL OR EMP.ToDate = ''1900-01-01 00:00:00.000'')
) S
PIVOT
(
MAX(AttributeTypeUnitDescription)
FOR AttributeTypeDescription IN ('+@AttributeColumn+')
) P';

INSERT INTO #VIEWEMPMST
EXEC sp_executesql @SQLSTRING;
 
END
 
DECLARE @OrgTbl NVARCHAR(50)

SET @OrgTbl = '##Orgtbl_' + REPLACE(REPLACE(CONVERT(VARCHAR(20), GETDATE(),109),' ',''),':','')

DECLARE @AttributeColumnTax NVARCHAR(MAX)

IF OBJECT_ID('tempdb..#AttributeTable') IS NOT NULL DROP TABLE #AttributeTable
 
SELECT	ITEMS
		,AttributeTypeCode
		,AttributeTypeDescription
INTO #AttributeTable
FROM Attribute.TypeConfiguration (NOLOCK) A
CROSS APPLY (SELECT Items FROM dbo.SplitByComma(AttributeBasedOn,'|')) B
INNER JOIN dbo.AttributeTypeMaster (NOLOCK) AT ON B.ITEMS = AT.AttributeTypeID AND AT.Applicable = 1
WHERE ModuleCode='CompensationAndBenefits'
AND SubModuleCode='CompensationAndBenefits'
AND A.Applicable = 1
 
 
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
INNER JOIN dbo.AttributeTypeUnitMaster (NOLOCK) AUT ON B.Items = AUT.AttributeTypeUnitID AND AUT.Applicable = 1
WHERE A.ModuleCode='CompensationAndBenefits'
AND A.SubModuleCode='CompensationAndBenefits'
AND A.Applicable = 1
 
 SELECT @AttributeColumnTax =
(
SELECT '['+AttributeTypeDescription+'],'
FROM #AttributeTable
FOR XML PATH('')
)

SET @AttributeColumnTax = LEFT(@AttributeColumnTax, LEN(@AttributeColumnTax)-1)
IF @AttributeColumnTax IS NOT NULL
BEGIN
SET @SQLSTRING=N'
SELECT CombinationID,PayGroupID,'+@AttributeColumnTax+'
INTO '+@OrgTbl+'
FROM
(
SELECT	B.CombinationID
		,PayGroupID
		,AttributeTypeDescription
		,AttributeTypeUnitDescription
FROM #AttributeTable A
INNER JOIN #AttributeUnitTable B ON A.Items = B.AttributeTypeID
) D

PIVOT
(
MAX(AttributeTypeUnitDescription)
FOR AttributeTypeDescription IN ('+@AttributeColumnTax+')
) P'
 
EXEC sp_executesql @SQLSTRING

END
 
 
DECLARE @Org1 VARCHAR(MAX)
SELECT @Org1 =
(
SELECT ' AND (A.['+ITEMS+']=B.['+ITEMS+'_Description])'
FROM ATTRIBUTE.TYPECONFIGURATION (NOLOCK) t
CROSS APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc,'|')) b
WHERE Applicable=1
AND ModuleCode='CompensationAndBenefits'
AND SubModuleCode='CompensationAndBenefits'
FOR XML PATH('')
)
 
 
DROP TABLE IF EXISTS ##Temp2
SET @SQLSTRING='
SELECT B.Empcode,A.*
INTO ##Temp2
FROM '+@OrgTbl+' A
INNER JOIN #VIEWEMPMST B ON 1=1 '+@Org1

EXEC sp_executesql @SQLSTRING
 
DROP TABLE IF EXISTS #FinalData

SELECT	ED_EMPID EmployeeID
		,ISNULL(ED_EMPCode,'') EmployeeCode
		,GPP_CountryID CountryId
		,ISNULL(ED_Salutation,'') Salutation
		,ISNULL(ED_FirstName,'') FirstName
		,ISNULL(ED_MiddleName,'') MiddleName
		,ISNULL(ED_LastName,'') LastName
		,ED_DOB DOB
		,CASE
			WHEN CONVERT(date,ED_DOJ) >= @Effective_start_date
			THEN CONVERT(date,ED_DOJ)
			ELSE @Effective_start_date
		 END DOJ
		,CASE
			WHEN CONVERT(date,ED_DOJ) >= @Effective_start_date
			THEN CONVERT(date,ED_DOJ)
			ELSE @Effective_start_date
		 END FromDate
		,CASE
			WHEN LOWER(ED_Sex)='male' THEN 1
			WHEN LOWER(ED_Sex)='female' THEN 2
			ELSE 3
		 END Gender
		,P.PayheadCode
		,P.PayHeadCategoryID
		,PM_PayheadID PayheadId
		,GPP_PropertyCode
		,ISNULL(PropertyChecked,0) PropertyChecked
		,ES.MonthlyRate
		,ES.YearlyValue YearlyRate
		,CONVERT(DATE,ES.ToDate) ToDate
		,CASE
			WHEN ES.EffDate IS NOT NULL
			THEN CONVERT(DATE, ES.FromDate)
			ELSE CONVERT(DATE, ES.EffDate)
		 END EffDate
		,LeenPayApp
		,TaxApp
		,SpotTaxApp
INTO #FinalData
FROM ##Temp2 A
INNER JOIN Common.GlobalPaygroupPayheadProperties B ON A.PaygroupID = B.APGD_PaygroupID AND GPP_CountryID = @CountryID
INNER JOIN dbo.Payheadmaster P ON B.PM_PayheadID = P.PayHeadID AND P.Applicable = 1
INNER JOIN COMMON.ATTRIBUTEPAYGROUPPAYHEADDETAILS ATT ON ATT.PayHeadID = P.PayHeadID
INNER JOIN dbo.EmpSALARY_MASTER ES ON A.Empcode = ES.Empcode AND ES.Todate IS NULL AND P.PayHeadCode = ES.HeadCode
INNER JOIN dbo.ReqRec_EmployeeDetails ED ON ES.EmpCode = ED.ED_EMPCode
WHERE ES.MonthlyRate <> 0
AND ES.updateDate >= @TimestampStr
AND ED_Status IN (1,10)  
  
DECLARE @cols NVARCHAR(MAX)=''
DECLARE @sql NVARCHAR(MAX)=''
 
SELECT @cols = STRING_AGG(QUOTENAME(GPP_PropertyCode),',')
FROM (SELECT DISTINCT GPP_PropertyCode FROM #FinalData) X
 
SET @cols = ELCM_CommonCore.ZTP.ProperCaseCSV(@cols)
 
SET @sql='
SELECT EmployeeID,EmployeeCode,CountryId,Salutation,FirstName,MiddleName,LastName,DOB,DOJ,Gender,PayheadCode,PayHeadCategoryID,PayheadId,MonthlyRate,YearlyRate,FromDate,ToDate,EffDate,LeenPayApp,TaxApp,SpotTaxApp,'+@cols+'
FROM
(
SELECT EmployeeID,EmployeeCode,CountryId,Salutation,FirstName,MiddleName,LastName,DOB,DOJ,Gender,PayheadCode,PayHeadCategoryID,PayheadId,MonthlyRate,YearlyRate,FromDate,ToDate,EffDate,LeenPayApp,TaxApp,SpotTaxApp,GPP_PropertyCode,ISNULL(CAST(PropertyChecked AS INT),0) PropertyChecked
FROM #FinalData
) SRC
PIVOT
(
MAX(PropertyChecked)
FOR GPP_PropertyCode IN ('+@cols+')
) P'
 
EXEC sp_executesql @sql 

END