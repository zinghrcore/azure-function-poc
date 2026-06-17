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

DECLARE @TimestampStr NVARCHAR(50),
		@COLUMNNAME VARCHAR(MAX),
		@AttributeColumn VARCHAR(2000),
		@SQLSTRING VARCHAR(MAX)

IF @LastTimestamp IS NULL
    SET @TimestampStr = CONVERT(VARCHAR, DATEADD(DAY, -90, GETDATE()), 120);
ELSE
    SET @TimestampStr = CONVERT(VARCHAR, @LastTimestamp, 121);

IF OBJECT_ID('tempdb..#VIEWEMPMST') IS NOT NULL DROP TABLE #VIEWEMPMST

CREATE TABLE #VIEWEMPMST
(
    EMPCODE VARCHAR(200)
)

SELECT @COLUMNNAME =
(
    SELECT ' ALTER TABLE #VIEWEMPMST ADD [' + ITEMS + '_Description] VARCHAR(2000) '
    FROM ATTRIBUTE.TYPECONFIGURATION (NOLOCK) t
    CROSS APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc, '|')) b
    WHERE Applicable = 1
    AND ModuleCode = 'CompensationAndBenefits'
    AND SubModuleCode = 'CompensationAndBenefits'
    FOR XML PATH('')
)

EXEC(@COLUMNNAME)

SELECT @AttributeColumn =
(
    SELECT '[' + Replace(ITEMS, ' ', '') + '_Description] ,'
    FROM ATTRIBUTE.TYPECONFIGURATION (NOLOCK) t
    CROSS APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc, '|')) b
    WHERE Applicable = 1
    AND ModuleCode = 'CompensationAndBenefits'
    AND SubModuleCode = 'CompensationAndBenefits'
    FOR XML PATH('')
)

SELECT @AttributeColumn = SUBSTRING(@AttributeColumn, 0, LEN(@AttributeColumn))

IF ISNULL(@AttributeColumn, '') <> ''
BEGIN
    SET @SQLSTRING = 'SELECT EmployeeCode,' + @AttributeColumn + '
    FROM
    (
        SELECT EmployeeCode,
        Replace(AT.AttributeTypeDescription, '' '', '''')+''_Description'' AS AttributeTypeDescription,
        ATU.AttributeTypeUnitDescription
        FROM dbo.EmployeeAttributeDetails (NOLOCK) EMP
        INNER JOIN AttributeTypeMaster (NOLOCK) AT
            ON AT.AttributeTypeID = Emp.AttributeTypeID
        INNER JOIN AttributeTypeUnitMaster (NOLOCK) ATU
            ON ATU.AttributeTypeUnitID = EMP.AttributeTypeUnitID
            AND Emp.AttributeTypeID = ATU.AttributeTypeID
        WHERE AT.Applicable = ATU.Applicable
        AND AT.Applicable = 1
        AND (EMP.ToDate IS NULL OR EMP.ToDate = ''1900-01-01 00:00:00.000'')
    ) AS S
    PIVOT
    (
        MAX(AttributeTypeUnitDescription)
        FOR AttributeTypeDescription IN (' + @AttributeColumn + ')
    ) AS PIV'

    INSERT INTO #VIEWEMPMST
    EXEC(@SQLSTRING)
END

DECLARE @OrgTbl VARCHAR(50)
SET @OrgTbl = '##Orgtbl_' + REPLACE(REPLACE(CONVERT(VARCHAR(20), GETDATE(), 109), ' ', ''), ':', '')

DECLARE @AttributeColumnTax VARCHAR(MAX)

IF OBJECT_ID('tempdb..#AttributeTable') IS NOT NULL DROP TABLE #AttributeTable

SELECT ITEMS, AttributeTypeCode, AttributeTypeDescription
INTO #AttributeTable
FROM Attribute.TypeConfiguration (NOLOCK) A
CROSS APPLY (SELECT Items FROM [dbo].[SplitByComma](AttributeBasedOn, '|')) B
INNER JOIN DBO.AttributeTypemaster (NOLOCK) AT ON B.ITEMS = AT.AttributeTypeID AND AT.Applicable = 1
WHERE ModuleCode = 'CompensationAndBenefits'
AND SubModuleCode = 'CompensationAndBenefits'
AND A.Applicable = 1
ORDER BY AttributeTypeID

IF OBJECT_ID('tempdb..#AttributeUnitTable') IS NOT NULL DROP TABLE #AttributeUnitTable

SELECT CombinationID, PayGroupID, Items, AttributeTypeID,AttributeTypeUnitCode, AttributeTypeUnitDescription
INTO #AttributeUnitTable
FROM ATTRIBUTE.UNITTYPECONFIGURATION (NOLOCK) A
INNER JOIN Common.PayGroupMaster (NOLOCK) P ON A.CombinationID = P.ComboID AND A.Applicable = P.Applicable
CROSS APPLY (SELECT Items FROM [dbo].[SplitByComma](AttributeUnitConfig, '|')) B
INNER JOIN DBO.AttributeTypeUnitMaster (NOLOCK) AUT ON B.Items = AUT.AttributeTypeUnitID AND AUT.Applicable = 1
WHERE A.ModuleCode = 'CompensationAndBenefits'
AND A.SubModuleCode = 'CompensationAndBenefits'
AND A.Applicable = 1
ORDER BY 3

SELECT @AttributeColumnTax =
(
    SELECT '[' + AttributeTypeDescription + '],'
    FROM #AttributeTable
    FOR XML PATH('')
)

SET @AttributeColumnTax = SUBSTRING(@AttributeColumnTax, 0, LEN(@AttributeColumnTax))

IF @AttributeColumnTax IS NOT NULL
BEGIN
    SET @SQLSTRING = 'SELECT CombinationID, PayGroupID,' + @AttributeColumnTax + '
    INTO [' + @OrgTbl + ']
    FROM
    (
        SELECT B.CombinationID, PayGroupID,
        AttributeTypeDescription, AttributeTypeUnitDescription
        FROM #AttributeTable A
        INNER JOIN #AttributeUnitTable B
            ON A.Items = B.AttributeTypeID
    ) AS D
    PIVOT
    (
        MAX(AttributeTypeUnitDescription)
        FOR AttributeTypeDescription IN (' + @AttributeColumnTax + ')
    ) AS p'

    EXEC(@SQLSTRING)
END
ELSE
BEGIN
    SELECT 'Paygroup is not Configured'
END

DECLARE @Org1 VARCHAR(MAX)

SET @Org1 =
(
    SELECT ' AND (A.[' + ITEMS + ']=b.[' + ITEMS + '_Description]) '
    FROM ATTRIBUTE.TYPECONFIGURATION (NOLOCK) t
    CROSS APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc, '|')) b
    WHERE Applicable = 1
    AND ModuleCode = 'CompensationAndBenefits'
    AND SubModuleCode = 'CompensationAndBenefits'
    FOR XML PATH('')
)

DROP TABLE IF EXISTS ##Temp2

SET @SQLSTRING = 'SELECT B.Empcode, A.* INTO ##Temp2
FROM [' + @OrgTbl + '] A
INNER JOIN #VIEWEMPMST B ON 1=1 ' + @Org1

EXEC(@SQLSTRING)

SELECT
    ED_EMPID                                                AS EmployeeID,
    ES.updateDate                                           AS updateDate_timestamp,
    ISNULL(ED_EMPCode, '')                                  AS EmployeeCode,
    ED_CountryID                                            AS CountryId,
    ISNULL(ED_Salutation, '')                               AS Salutation,
    ISNULL(ED_FirstName, '')                                AS FirstName,
    ISNULL(ED_MiddleName, '')                               AS MiddleName,
    ISNULL(ED_LastName, '')                                 AS LastName,
    EFD_StateId                                             AS StateID,
    ED_DOB                                                  AS DOB,
    CONVERT(date, ES.FROMDATE)                              AS FromDate,
    (CASE
        WHEN LOWER(ED_Sex) = 'male'   THEN 1
        WHEN LOWER(ED_Sex) = 'female' THEN 2
        ELSE 3
     END)                                                   AS Gender,
    P.PayHeadCode                                           AS PayHead,
    P.PayHeadID,
    P.PayHeadCategoryID,
    ES.MonthlyRate                                          AS MonthlyAmount,
    ES.YearlyValue                                          AS YearlyAmount,
       CONVERT(date, ES.ToDate)                                AS ToDate,
    CONVERT(date, ISNULL(ES.EffDate, ES.FROMDATE))          AS EffDate,
    APG.PfApp                                               AS IsPFApplicable,
    EFD.EFD_PFApplicable                                    AS IsPFApplicableForEmployee,
    APG.EsicApp                                             AS IsESICApplicable,
    EFD.EFD_ESICApplicable                                  AS IsESICApplicableForEmployee,
    APG.PtApp                                               AS IsPTApplicable,
    EFD.EFD_PTApplicable                                    AS IsPTApplicableForEmployee,
    APG.LwfApp                                              AS IsLWFApplicable,
    EFD.EFD_LwfApplicable                                   AS IsLWFApplicableForEmployee,
    APG.PfApp                                               AS IsVPFApplicable,
    EFD.EFD_VPFApplicable                                   AS IsVPFApplicableForEmployee,
    APG.OTApplicable                                        AS IsOTApplicable,
    APG.TaxApp                                              AS IsTaxApplicable,
    APG.SpotTaxApp,
    APG.GrossNetApp,
    APG.GratuityApp                                         AS gratuity_applicable,
    APG.LeenPayApp
FROM ##Temp2 A
INNER JOIN Common.AttributePayGroupPayheadDetails (NOLOCK) APG ON A.PayGroupID = APG.PayGroupID
INNER JOIN dbo.Payheadmaster (NOLOCK) P ON APG.PayHeadID = P.PayHeadID AND P.Applicable = 1
INNER JOIN dbo.EmpSALARY_MASTER (NOLOCK) ES ON A.Empcode = ES.Empcode AND ES.Todate IS NULL AND P.PayHeadCode = ES.HeadCode
INNER JOIN dbo.ReqRec_EmployeeDetails (NOLOCK) ED ON ES.EmpCode = ED.ED_EMPCode
INNER JOIN dbo.ReqRec_EmployeeFinDetails (NOLOCK) EFD ON ED.ED_EMPID = EFD.EFD_ED_EMPID
WHERE ES.MonthlyRate <> 0
AND ED.ED_Status IN (1,10)
AND ES.[updateDate] >= @TimestampStr

END