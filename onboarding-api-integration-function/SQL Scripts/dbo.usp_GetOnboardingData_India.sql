
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

--EXEC [dbo].[usp_GetOnboardingData] @LastTimestamp = '2026-05-05 11:00:00.000'

CREATE OR ALTER PROCEDURE [dbo].[usp_GetOnboardingData]
(
    @LastTimestamp DATETIME = NULL
)
AS
BEGIN


SET NOCOUNT ON;
SET XACT_ABORT ON;


IF OBJECT_ID('tempdb..#ChangedEmp') IS NOT NULL DROP TABLE #ChangedEmp;

SELECT DISTINCT 
    EmpCode,
    UpdateDate
INTO #ChangedEmp
FROM dbo.EmpSALARY_MASTER WITH (NOLOCK)
WHERE (ISNULL(@LastTimestamp,'') = '' OR UpdateDate > @LastTimestamp);

IF NOT EXISTS (SELECT 1 FROM #ChangedEmp)
BEGIN
    SELECT TOP 0 * FROM dbo.EmpSALARY_MASTER;
    RETURN;
END

DECLARE @COLUMNNAME VARCHAR(MAX)
		,@AttributeColumn VARCHAR(2000)
		,@SQLSTRING VARCHAR(MAX) 

IF OBJECT_ID('tempdb..#VIEWEMPMST') IS NOT NULL DROP TABLE #VIEWEMPMST

CREATE TABLE #VIEWEMPMST
(EMPCODE VARCHAR(200))

SELECT @COLUMNNAME  = (
						SELECT ' ALTER TABLE #VIEWEMPMST ADD ['+ITEMS +'_Description] VARCHAR(2000) ' 
						FROM ATTRIBUTE.TYPECONFIGURATION(NOLOCK) t
						CROSS APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc,'|'))b
						WHERE Applicable=1 AND ModuleCode='CompensationAndBenefits' 
						AND SubModuleCode='CompensationAndBenefits'
						FOR XML PATH('') 
					  ) 

EXEC(@COLUMNNAME)

SELECT @AttributeColumn = (
							SELECT '['+Replace(ITEMS,' ','') +'_Description] ,' 
							FROM ATTRIBUTE.TYPECONFIGURATION(NOLOCK) t
							CROSS APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc,'|'))b
							WHERE Applicable=1 AND ModuleCode='CompensationAndBenefits' 
							AND SubModuleCode='CompensationAndBenefits'
							FOR XML PATH('')
						  )

SELECT @AttributeColumn = SUBSTRING(@AttributeColumn,0,LEN(@AttributeColumn))


IF ISNULL(@AttributeColumn ,'') <> ''
BEGIN 
	SET @SQLSTRING =''
	SET @SQLSTRING ='SELECT EmployeeCode,' + @AttributeColumn + ' 
	FROM 
	(
	SELECT EmployeeCode,Replace(AT.AttributeTypeDescription,'' '','''')+''_Description'' AS AttributeTypeDescription ,ATU.AttributeTypeUnitDescription 
	FROM dbo.EmployeeAttributeDetails(NOLOCK) EMP 
	INNER JOIN AttributeTypeMaster(NOLOCK) AT ON AT.AttributeTypeID=Emp.AttributeTypeID
	INNER JOIN AttributeTypeUnitMaster(NOLOCK) ATU ON ATU.AttributeTypeUnitID = EMP.AttributeTypeUnitID AND Emp.AttributeTypeID = ATU.AttributeTypeID
	WHERE AT.Applicable = ATU.Applicable  And AT.Applicable = 1 AND (EMP.ToDate IS NULL OR EMP.ToDate =''1900-01-01 00:00:00.000'') AND EMP.EmployeeCode IN (SELECT EmpCode FROM #ChangedEmp)
	) AS S
	PIVOT
	(
	MAX(AttributeTypeUnitDescription) FOR AttributeTypeDescription IN ( ' + @AttributeColumn + ')
	)AS PIV'

	INSERT INTO #VIEWEMPMST
	EXEC(@SQLSTRING)
END 

DECLARE @OrgTbl VARCHAR(50)
Declare @AttributeColumnTax Varchar(MAX)

SET @OrgTbl = '##Orgtbl_' + REPLACE(REPLACE(CONVERT(VARCHAR(20), GETDATE(), 109) , ' ', ''), ':','')


IF OBJECT_ID('tempdb..#AttributeTable') IS NOT NULL DROP TABLE #AttributeTable

SELECT	ITEMS
		,AttributeTypeCode
		,AttributeTypeDescription
INTO #AttributeTable
FROM Attribute.TypeConfiguration(NOLOCK) A 
CROSS APPLY (SELECT Items FROM [dbo].[SplitByComma](AttributeBasedOn,'|')) B 
INNER JOIN DBO.AttributeTypemaster(NOLOCK) AT ON B.ITEMS = At.AttributeTypeID AND AT.Applicable=1 
WHERE ModuleCode='CompensationAndBenefits' 
AND SubModuleCode='CompensationAndBenefits' 
AND A.Applicable=1
ORDER BY AttributeTypeID 


IF OBJECT_ID('tempdb..#AttributeUnitTable') IS NOT NULL DROP TABLE #AttributeUnitTable

SELECT	CombinationID
		,PayGroupID
		,Items
		,AttributeTypeID
		,AttributeTypeUnitCode
		,AttributeTypeUnitDescription
INTO #AttributeUnitTable
FROM ATTRIBUTE.UNITTYPECONFIGURATION(NOLOCK) A
INNER JOIN Common.PayGroupMaster(NOLOCK) P On A.CombinationID = P.ComboID AND A.Applicable =P.Applicable 
CROSS APPLY (SELECT Items FROM [dbo].[SplitByComma](AttributeUnitConfig,'|')) B 
INNER JOIN DBO.AttributeTypeUnitMaster(NOLOCK) AUT ON B.Items=AUT.AttributeTypeUnitID AND AUT.Applicable=1
WHERE A.ModuleCode='CompensationAndBenefits' AND A.SubModuleCode='CompensationAndBenefits' AND A.Applicable=1 
ORDER BY 3 

SELECT @AttributeColumnTax =
( 
Select '['+AttributeTypeDescription+'],' 
from #AttributeTable
FOR XML PATH('')
)
SET @AttributeColumnTax = SUBSTRING(@AttributeColumnTax,0,LEN(@AttributeColumnTax))

IF @AttributeColumnTax IS NOT NULL 
BEGIN 

	SET @SqlString =''
	SET @SqlString ='SELECT CombinationID,PayGroupID,'+@AttributeColumnTax+'
	INTO ['+@OrgTbl+']
	FROM 
	(
	SELECT B.CombinationID,PayGroupID,AttributeTypeDescription,AttributeTypeUnitDescription
	FROM #AttributeTable A
	INNER JOIN #AttributeUnitTable B ON A.Items = B.AttributeTypeID
	WHERE 1=1
	) AS D 
	PIVOT (MAX(AttributeTypeUnitDescription) FOR AttributeTypeDescription IN  ('+@AttributeColumnTax+')) as p
	'
	Exec(@SqlString)	
END
ELSE
BEGIN 
	SELECT 'Paygroup is not Configured'
END 


Declare @Org1 VARCHAR(MAX)
SET @Org1 =''

SET @Org1 = (
			 SELECT ' AND (A.['+ITEMS+']=b.['+ITEMS+'_Description]) ' 
			 FROM ATTRIBUTE.TYPECONFIGURATION(NOLOCK) t
			 CROSS APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc,'|'))b
			 WHERE Applicable=1 AND ModuleCode='CompensationAndBenefits' 
			 AND SubModuleCode='CompensationAndBenefits' 
			 FOR XML PATH('')
			)

--Print @Org1

DROP TABLE IF EXISTS ##Temp2

SET @SqlString =''
SET @SqlString ='SELECT B.Empcode,A.*  INTO ##Temp2
				 FROM ['+@OrgTbl+'] A 
				 INNER JOIN #VIEWEMPMST B ON 1=1 ' + @Org1 + ''

--Print @SqlString

EXEC(@SqlString)


SELECT	'Daily' InputType
		,ED_EMPID EmployeeID
		,ISNULL(ED_EMPCode,'') EmployeeCode
		,ED_CountryID CountryId
		,ISNULL(ED_Salutation,'') Salutation
		,ISNULL(ED_FirstName,'') FirstName
		,ISNULL(ED_MiddleName, '') MiddleName
		,ISNULL(ED_LastName,'') LastName
		,EFD_StateId StateID
		,ED_DOB DOB
		,ED_DOJ DOJ
		,ED_Sex Gender
		,P.PayHeadCode PayHead
		,P.PayHeadID
		,P.PayHeadCategoryID
		,MonthlyRate MonthlyAmount
		,YearlyValue YearlyAmount
		,ES.FromDate FromDate
		,ES.ToDate ToDate
		,ES.effDate
		,ED_Status
		,ED_DOL
		,PfApp IsPFApplicable
		,EFD_PFApplicable AS IsPFApplicableForEmployee
		,EsicApp AS IsESICApplicable
		,EFD_ESICApplicable  IsESICApplicableForEmployee
		,PtApp IsPTApplicable
		,EFD_PTApplicable IsPTApplicableForEmployee
		,LwfApp IsLWFApplicable
		,EFD_LwfApplicable IsLWFApplicableForEmployee
		,PfApp IsVPFApplicable
		,EFD_VPFApplicable AS IsVPFApplicableForEmployee
		,OTApplicable [IsOTApplicable]
		,TaxApp [IsTaxApplicable]
		,SpotTaxApp
		,GrossNetApp
		,GratuityApp [gratuity_applicable]
FROM ##Temp2 A 
INNER JOIN Common.AttributePayGroupPayheadDetails(NOLOCK) APG ON A.PaygroupID = APG.PayGroupID 
INNER JOIN dbo.Payheadmaster(NOLOCK) P ON APG.PayheadID = P.PayHeadID and P.Applicable =1 
INNER JOIN dbo.EmpSALARY_MASTER(NOLOCK) ES ON A.Empcode = ES.Empcode and ES.Todate IS NULL and P.PayHeadCode = ES.HeadCode 
INNER JOIN dbo.ReqRec_EmployeeDetails(NOLOCK) ED ON ES.EmpCode = ED.ED_EMPCode
INNER JOIN dbo.ReqRec_EmployeeFinDetails(NOLOCK) EFD ON ED.ED_EMPID = EFD.EFD_ED_EMPID
WHERE ES.MonthlyRate <> 0 AND GrossNetApp=1 and ED.ED_Status in (1,10,5,6)
AND ES.EmpCode IN (SELECT EmpCode FROM #ChangedEmp)
ORDER BY ED.ED_EMPID

END