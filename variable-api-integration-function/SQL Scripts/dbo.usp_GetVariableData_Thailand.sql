
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

------------------------------------------------------------
-- STEP 1: WATERMARK FILTER (CDC BASE)
------------------------------------------------------------
DECLARE @TimestampStr NVARCHAR(50)

IF @LastTimestamp IS NULL
    SET @TimestampStr = CONVERT(VARCHAR, DATEADD(DAY, -90, GETDATE()), 120);
ELSE
    SET @TimestampStr = CONVERT(VARCHAR, @LastTimestamp, 121);

DECLARE @Effective_start_date DATE = DATEFROMPARTS(YEAR(GETDATE())-1, 1, 1)	--2025-01-01
		,@CountryID INT = (SELECT CountryID FROM ELCM_COMMONCORE.dbo.signupdetails WHERE organizationName = (SELECT REPLACE(DB_NAME(),'ELCM_','')))
		,@COLUMNNAME VARCHAR(MAX)
        ,@AttributeColumn VARCHAR(2000)
        ,@SQLSTRING VARCHAR(MAX)

-- ============================================================
-- #VIEWEMPMST  (Employee Attribute Pivot)
-- ============================================================

IF OBJECT_ID('tempdb..#VIEWEMPMST') IS NOT NULL DROP TABLE #VIEWEMPMST

CREATE TABLE #VIEWEMPMST (EMPCODE VARCHAR(200))

SELECT @COLUMNNAME =
(
    SELECT ' ALTER TABLE #VIEWEMPMST ADD ['+ITEMS+'_Description] VARCHAR(2000)'
    FROM   ATTRIBUTE.TYPECONFIGURATION(NOLOCK) t
    CROSS  APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc,'|')) b
    WHERE  Applicable=1
      AND  ModuleCode='CompensationAndBenefits'
      AND  SubModuleCode='CompensationAndBenefits'
    FOR XML PATH('')
)

EXEC(@COLUMNNAME)

SELECT @AttributeColumn =
(
    SELECT '['+REPLACE(ITEMS,' ','')+'_Description],'
    FROM   ATTRIBUTE.TYPECONFIGURATION(NOLOCK) t
    CROSS  APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc,'|')) b
    WHERE  Applicable=1
      AND  ModuleCode='CompensationAndBenefits'
      AND  SubModuleCode='CompensationAndBenefits'
    FOR XML PATH('')
)

SET @AttributeColumn = SUBSTRING(@AttributeColumn, 0, LEN(@AttributeColumn))

IF ISNULL(@AttributeColumn, '') <> ''
BEGIN
    SET @SQLSTRING =
    '
    SELECT EmployeeCode,' + @AttributeColumn + '
    FROM
    (
        SELECT EmployeeCode,
               REPLACE(AT.AttributeTypeDescription,'' '','''')+''_Description'' AS AttributeTypeDescription,
               ATU.AttributeTypeUnitDescription
        FROM   dbo.EmployeeAttributeDetails EMP (NOLOCK)
        INNER JOIN AttributeTypeMaster AT (NOLOCK)
            ON AT.AttributeTypeID = EMP.AttributeTypeID
        INNER JOIN AttributeTypeUnitMaster ATU (NOLOCK)
            ON  ATU.AttributeTypeUnitID = EMP.AttributeTypeUnitID
            AND EMP.AttributeTypeID     = ATU.AttributeTypeID
        WHERE AT.Applicable = ATU.Applicable
          AND AT.Applicable = 1
          AND (EMP.ToDate IS NULL OR EMP.ToDate = ''1900-01-01 00:00:00.000'')
    ) S
    PIVOT
    (
        MAX(AttributeTypeUnitDescription)
        FOR AttributeTypeDescription IN (' + @AttributeColumn + ')
    ) PIV
    '

    INSERT INTO #VIEWEMPMST
	EXEC(@SQLSTRING)
END

-- ============================================================
-- ORG ATTRIBUTE PIVOT
-- ============================================================

DECLARE @OrgTbl VARCHAR(100)
SET     @OrgTbl = '##Orgtbl'

DECLARE @AttributeColumnTax VARCHAR(MAX)

IF OBJECT_ID('tempdb..#AttributeTable') IS NOT NULL DROP TABLE #AttributeTable

SELECT   ITEMS, AttributeTypeCode, AttributeTypeDescription
INTO     #AttributeTable
FROM     Attribute.TypeConfiguration A (NOLOCK)
CROSS    APPLY (SELECT Items FROM dbo.SplitByComma(AttributeBasedOn, '|')) B
INNER    JOIN AttributeTypeMaster AT (NOLOCK)
    ON   B.ITEMS = AT.AttributeTypeID AND AT.Applicable = 1
WHERE    ModuleCode    = 'CompensationAndBenefits'
  AND    SubModuleCode = 'CompensationAndBenefits'
  AND    A.Applicable  = 1

IF OBJECT_ID('tempdb..#AttributeUnitTable') IS NOT NULL DROP TABLE #AttributeUnitTable

SELECT   CombinationID, PayGroupID, Items,
         AttributeTypeID, AttributeTypeUnitCode, AttributeTypeUnitDescription
INTO     #AttributeUnitTable
FROM     ATTRIBUTE.UNITTYPECONFIGURATION A (NOLOCK)
INNER    JOIN Common.PayGroupMaster P (NOLOCK)
    ON   A.CombinationID = P.ComboID AND A.Applicable = P.Applicable
CROSS    APPLY (SELECT Items FROM dbo.SplitByComma(AttributeUnitConfig, '|')) B
INNER    JOIN AttributeTypeUnitMaster AUT (NOLOCK)
    ON   B.Items = AUT.AttributeTypeUnitID AND AUT.Applicable = 1
WHERE    A.ModuleCode    = 'CompensationAndBenefits'
  AND    A.SubModuleCode = 'CompensationAndBenefits'
  AND    A.Applicable    = 1

SELECT @AttributeColumnTax =
(
    SELECT '[' + AttributeTypeDescription + '],'
    FROM   #AttributeTable
    FOR XML PATH('')
)

SET @AttributeColumnTax = SUBSTRING(@AttributeColumnTax, 0, LEN(@AttributeColumnTax))

IF @AttributeColumnTax IS NOT NULL
BEGIN
    SET @SQLSTRING =
    '
    SELECT CombinationID, PayGroupID,' + @AttributeColumnTax + '
    INTO   [' + @OrgTbl + ']
    FROM
    (
        SELECT B.CombinationID, PayGroupID,
               AttributeTypeDescription, AttributeTypeUnitDescription
        FROM   #AttributeTable  A
        INNER  JOIN #AttributeUnitTable B ON A.Items = B.AttributeTypeID
    ) D
    PIVOT
    (
        MAX(AttributeTypeUnitDescription)
        FOR AttributeTypeDescription IN (' + @AttributeColumnTax + ')
    ) P
    '

    EXEC(@SQLSTRING)
END

-- ============================================================
-- ORG JOIN
-- ============================================================

DECLARE @Org1 VARCHAR(MAX)

SET @Org1 =
(
    SELECT ' AND (A.[' + ITEMS + ']=B.[' + ITEMS + '_Description])'
    FROM   ATTRIBUTE.TYPECONFIGURATION(NOLOCK) t
    CROSS  APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc, '|')) b
    WHERE  Applicable    = 1
      AND  ModuleCode    = 'CompensationAndBenefits'
      AND  SubModuleCode = 'CompensationAndBenefits'
    FOR XML PATH('')
)

DROP TABLE IF EXISTS ##Temp2

SET @SQLSTRING =
'
SELECT B.Empcode, A.*
INTO   ##Temp2
FROM   [' + @OrgTbl + '] A
INNER  JOIN #VIEWEMPMST B ON 1=1 ' + @Org1

EXEC(@SQLSTRING)

-- ============================================================
-- FINAL DATA (#FinalData)
-- ============================================================

DROP TABLE IF EXISTS #FinalData

SELECT
    ED_EMPID                                                AS EmployeeID,
    ISNULL(ED_EMPCode, '')                                  AS EmployeeCode,
    GPP_CountryID                                           AS CountryId,
    ISNULL(ED_Salutation, '')                               AS Salutation,
    ISNULL(ED_FirstName, '')                                AS FirstName,
    ISNULL(ED_MiddleName, '')                               AS MiddleName,
    ISNULL(ED_LastName, '')                                 AS LastName,
    ED_DOB                                                  AS DOB,
    ED_DOJ                                                  AS DOJ,
    CASE
        WHEN LOWER(ED_Sex) = 'male'   THEN 1
        WHEN LOWER(ED_Sex) = 'female' THEN 2
        ELSE 3
    END                                                     AS Gender,
    P.PayheadCode,
    P.PayHeadCategoryID,
    PM_PayheadID                                            AS PayheadId,
    GPP_PropertyCode,
    ISNULL(PropertyChecked, 0)                              AS PropertyChecked,
    ES.MonthlyRate,
    ES.YearlyValue                                          AS YearlyRate,
    CASE
        WHEN CONVERT(date, ED_DOJ) >= @Effective_start_date
        THEN CONVERT(date, ED_DOJ)
        ELSE @Effective_start_date
    END                                                     AS FromDate,
    CONVERT(date, ES.ToDate)                                AS ToDate,
    CONVERT(date, ES.EffDate)                               AS EffDate,
    LeenPayApp,
    TaxApp,
    SpotTaxApp
INTO #FinalData
FROM ##Temp2 A
INNER JOIN Common.GlobalPaygroupPayheadProperties B (NOLOCK)
    ON  A.PaygroupID    = B.APGD_PaygroupID
    AND GPP_CountryID   = @CountryID
INNER JOIN PayheadMaster P (NOLOCK)
    ON  B.PM_PayheadID  = P.PayHeadID
    AND P.Applicable    = 1
INNER JOIN COMMON.ATTRIBUTEPAYGROUPPAYHEADDETAILS ATT (NOLOCK)
    ON  ATT.PayHeadID   = P.PayHeadID
INNER JOIN EmpSALARY_MASTER ES
    ON  A.Empcode       = ES.EmpCode
    AND ES.ToDate       IS NULL
    AND P.PayHeadCode   = ES.HeadCode
INNER JOIN ReqRec_EmployeeDetails ED (NOLOCK)
    ON  ES.EmpCode      = ED.ED_EMPCode
WHERE ES.MonthlyRate <> 0
 

-- ============================================================
-- PROPERTY PIVOT  +  FINAL OUTPUT
-- ============================================================

DECLARE @cols NVARCHAR(MAX) = ''
DECLARE @sql  NVARCHAR(MAX) = ''

SELECT @cols = STRING_AGG(QUOTENAME(GPP_PropertyCode), ',')
FROM (SELECT DISTINCT GPP_PropertyCode FROM #FinalData) x

SET @cols = ZTP.ProperCaseCSV(@cols)

-- Build helper SQL expressions for PF/SSC/VPF property columns.
-- These flags come from the pivoted #EmployeewiseApplicability (EA), NOT from
-- GlobalPay.EmpVariable (which has no such columns).
-- We look up each property column by checking what was actually pivoted.
-- If the column doesn't exist in the pivot, we default to 0.

DECLARE @PFCol  NVARCHAR(200) = ''
DECLARE @SSCCol NVARCHAR(200) = ''
DECLARE @VPFCol NVARCHAR(200) = ''

-- Resolve PF column  -> SSSEMPLOYEE  (Philippine SSS = social security / PF equivalent)
SELECT @PFCol = QUOTENAME(GPP_PropertyCode)
FROM (SELECT DISTINCT GPP_PropertyCode FROM #FinalData) x
WHERE UPPER(GPP_PropertyCode) IN ('SSSEMPLOYEE','PFAPPLICABLE','PF','PFEMPLOYEE','PF_EMPLOYEE','PF_APPLICABLE')

-- Resolve SSC column -> PHILHEALTHEMPLOYEE  (PhilHealth = health/SSC equivalent)
SELECT @SSCCol = QUOTENAME(GPP_PropertyCode)
FROM (SELECT DISTINCT GPP_PropertyCode FROM #FinalData) x
WHERE UPPER(GPP_PropertyCode) IN ('PHILHEALTHEMPLOYEE','SSCAPPLICABLE','SSC','SSCEMPLOYEE','SSC_EMPLOYEE','SSC_APPLICABLE')

-- Resolve VPF column -> PAGIBIGHDMFEMPLOYEE  (Pag-IBIG HDMF = voluntary provident fund)
SELECT @VPFCol = QUOTENAME(GPP_PropertyCode)
FROM (SELECT DISTINCT GPP_PropertyCode FROM #FinalData) x
WHERE UPPER(GPP_PropertyCode) IN ('PAGIBIGHDMFEMPLOYEE','VPFAPPLICABLE','VPF','VPFEMPLOYEEAPPLICABLE','VPF_EMPLOYEE','VPF_APPLICABLE','VOLUNTARYPF')

-- If not found as a property, default the expression to literal 0
IF @PFCol  = '' SET @PFCol  = '0'
IF @SSCCol = '' SET @SSCCol = '0'
IF @VPFCol = '' SET @VPFCol = '0'

SET @sql = '
-- Step A: pivot property flags into #EmployeewiseApplicability
SELECT EmployeeID, EmployeeCode, CountryId,
       Salutation, FirstName, MiddleName, LastName,
       DOB, DOJ, Gender,
       PayheadCode, PayHeadCategoryID, PayheadId,
       MonthlyRate, YearlyRate, FromDate, ToDate,
       EffDate, LeenPayApp, TaxApp, SpotTaxApp, ' + @cols + '
INTO   #EmployeewiseApplicability
FROM
(
    SELECT EmployeeID, EmployeeCode, CountryId,
           Salutation, FirstName, MiddleName, LastName,
           DOB, DOJ, Gender,
           PayheadCode, PayHeadCategoryID, PayheadId,
           MonthlyRate, YearlyRate, FromDate, ToDate,
           EffDate, LeenPayApp, TaxApp, SpotTaxApp,
           GPP_PropertyCode,
           ISNULL(CAST(PropertyChecked AS INT), 0) PropertyChecked
    FROM   #FinalData
) src
PIVOT
(
    MAX(PropertyChecked)
    FOR GPP_PropertyCode IN (' + @cols + ')
) p

-- Step B: return variable-pay rows filtered by CDC watermark.
-- PFApplicable / SSCApplicable / VPFApplicable are sourced from EA (the
-- pivoted property table). GlobalPay.EmpVariable has no such columns.
-- TaxApp / SpotTaxApp come from EA (originally from #FinalData via ATT join).
-- LeenPayApp is fetched for internal use but excluded from the API payload.
SELECT DISTINCT
    ED_EMPID                                                AS EmployeeID,
    EV.EmpCode                                              AS EmployeeCode,
    P.PayHeadID,
    EV.HeadCode                                             AS PayheadCode,
    P.PayHeadCategoryID,
    EV.CreatedDate                                          AS ApprovedDate,
    CASE WHEN P.PayHeadCategoryID = 2
         THEN EV.Amount * -1
         ELSE EV.Amount
    END                                                     AS Amount,
    1                                                       AS Type,
    ISNULL(' + @PFCol  + ', 0)                             AS PFApplicable,
    ISNULL(' + @SSCCol + ', 0)                             AS SSCApplicable,
    ISNULL(' + @VPFCol + ', 0)                             AS VPFApplicable,
    EA.LeenPayApp,
    EA.TaxApp                                               AS TaxApplicable,
    EA.SpotTaxApp                                           AS SpotTaxApplicable
FROM   GlobalPay.EmpVariable EV (NOLOCK)
INNER  JOIN PayHeadMaster P (NOLOCK)
    ON  P.PayHeadCode = EV.HeadCode
INNER  JOIN ReqRec_EmployeeDetails ED (NOLOCK)
    ON  EV.EmpCode = ED.ED_EMPCode
INNER  JOIN #EmployeewiseApplicability EA
    ON  ED.ED_EMPID = EA.EmployeeID
WHERE  1 = 1
--AND EV.CreatedDate >= ' + @TimestampStr+ '
ORDER  BY ED.ED_EMPID
'

EXEC sp_executesql @sql

-- ============================================================
-- CLEANUP
-- ============================================================

DROP TABLE IF EXISTS ##Temp2

IF OBJECT_ID('tempdb..' + @OrgTbl) IS NOT NULL
    EXEC('DROP TABLE ' + @OrgTbl)

END