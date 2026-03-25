
-- EXEC dbo.usp_GetOnboardingData @LastTimestamp = '2026-03-25 12:05:26.973'
-- Hardcoded dates are added for Testing for DOJ and FromDate Fields

CREATE OR ALTER PROCEDURE dbo.usp_GetOnboardingData
    @LastTimestamp DATETIME = NULL
AS
BEGIN
    SET NOCOUNT ON;

	-----------------------------------------
    -- STEP 1: GET CHANGED EMPLOYEES (CDC)
    -----------------------------------------

    IF OBJECT_ID('tempdb..#ChangedEmp') IS NOT NULL DROP TABLE #ChangedEmp;

    SELECT DISTINCT 
        EmpCode,
        updateDate
    INTO #ChangedEmp
    FROM dbo.EmpSALARY_MASTER WITH (NOLOCK)
    WHERE (@LastTimestamp IS NULL OR updateDate > @LastTimestamp);

    -----------------------------------------
    -- STEP 2: EXIT IF NO DATA
    -----------------------------------------

    IF NOT EXISTS (SELECT 1 FROM #ChangedEmp)
    BEGIN
        SELECT TOP 0 * FROM dbo.EmpSALARY_MASTER; -- empty result
        RETURN;
    END

    -----------------------------------------
    -- STEP 3: MAIN DATA FETCH
    -----------------------------------------

	IF OBJECT_ID('tempdb..#FinalData') IS NOT NULL DROP TABLE #FinalData;

	DECLARE @COLUMNNAME VARCHAR(MAX)
			,@AttributeColumn VARCHAR(2000)
			,@SQLSTRING VARCHAR(MAX) 
			,@SQLSTRING1 VARCHAR(MAX) 
			--,@UserID VARCHAR(100)='902534'
	
	IF OBJECT_ID('tempdb..#VIEWEMPMST') IS NOT NULL DROP TABLE #VIEWEMPMST
	        
	CREATE TABLE #VIEWEMPMST(EMPCODE VARCHAR(200))
	        
	SELECT @COLUMNNAME  = (SELECT ' ALTER TABLE #VIEWEMPMST ADD ['+ITEMS +'_Description] VARCHAR(2000) '
							FROM ATTRIBUTE.TYPECONFIGURATION(NOLOCK) t
							CROSS APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc,'|'))b
							WHERE Applicable=1 AND ModuleCode='CompensationAndBenefits'
							AND SubModuleCode='CompensationAndBenefits'
							FOR XML PATH('') )
	        
	-- SET @COLUMNNAME = SUBSTRING (@COLUMNNAME ,-1,LEN(@COLUMNNAME) )
	-- PRINT @COLUMNNAME
	        
	EXEC(@COLUMNNAME)
	        
	SELECT @AttributeColumn = ( SELECT '['+Replace(ITEMS,' ','') +'_Description] ,'
								FROM ATTRIBUTE.TYPECONFIGURATION(NOLOCK) t
								CROSS APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc,'|'))b
								WHERE Applicable=1 AND ModuleCode='CompensationAndBenefits'
								AND SubModuleCode='CompensationAndBenefits'
								FOR XML PATH('')
							  )
	        
	SELECT @AttributeColumn = SUBSTRING(@AttributeColumn,0,LEN(@AttributeColumn))
	        
	---PRINT @AttributeColumn
	        
	IF ISNULL(@AttributeColumn ,'') <> ''
	BEGIN
	        
		SET @SQLSTRING =''
		SET @SQLSTRING ='SELECT EmployeeCode,' + @AttributeColumn + '
						 FROM
						 (
						 SELECT EmployeeCode,Replace(AT.AttributeTypeDescription,'' '','''')+''_Description'' AS	AttributeTypeDescription ,ATU.AttributeTypeUnitDescription
						 FROM dbo.EmployeeAttributeDetails(NOLOCK) EMP
						 INNER JOIN AttributeTypeMaster(NOLOCK) AT ON AT.AttributeTypeID=Emp.AttributeTypeID
						 INNER JOIN AttributeTypeUnitMaster(NOLOCK) ATU ON ATU.AttributeTypeUnitID = EMP.AttributeTypeUnitID AND	Emp.AttributeTypeID = ATU.AttributeTypeID
						 WHERE AT.Applicable = ATU.Applicable  And AT.Applicable = 1 AND (EMP.ToDate IS NULL OR EMP.ToDate =''1900-01-01 00:00:00.000'')
						 ) AS S
	        
						 PIVOT
	        
						 (
						 MAX(AttributeTypeUnitDescription) FOR AttributeTypeDescription IN ( ' + @AttributeColumn + ')
						 )AS PIV'
	        
						 --WHERE EmployeeCode ='''+@UserID+''''        
						 --PRINT @SQLSTRING
	
		INSERT INTO #VIEWEMPMST
		EXEC(@SQLSTRING)
	
	END
	
	DECLARE @OrgTbl VARCHAR(50)--,@SqlString VARCHAR(MAX)
	SET @OrgTbl = '##Orgtbl_' + REPLACE(REPLACE(CONVERT(VARCHAR(20), GETDATE(), 109) , ' ', ''), ':','')
	
	DECLARE @AttributeColumnTax VARCHAR(MAX)
	
	IF OBJECT_ID('tempdb..#AttributeTable') IS NOT NULL DROP TABLE #AttributeTable
	
	SELECT ITEMS,AttributeTypeCode,AttributeTypeDescription
	INTO #AttributeTable
	FROM Attribute.TypeConfiguration(NOLOCK) A
	CROSS APPLY (SELECT Items FROM [dbo].[SplitByComma](AttributeBasedOn,'|')) B
	INNER JOIN DBO.AttributeTypemaster(NOLOCK) AT ON B.ITEMS = At.AttributeTypeID AND AT.Applicable=1
	WHERE ModuleCode='CompensationAndBenefits' AND SubModuleCode='CompensationAndBenefits'
	AND A.Applicable=1
	ORDER BY AttributeTypeID
	        
	IF OBJECT_ID('tempdb..#AttributeUnitTable') IS NOT NULL DROP TABLE #AttributeUnitTable
	SELECT CombinationID,PayGroupID,Items,AttributeTypeID,AttributeTypeUnitCode,AttributeTypeUnitDescription        
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
	--PRINT @AttributeColumnTax
	        
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
					  
					  PIVOT (MAX(AttributeTypeUnitDescription) FOR AttributeTypeDescription IN  ('+@AttributeColumnTax+')) as p'
	        
	 --PRINT @SqlString
	 EXEC(@SqlString)
	END
	ELSE
	BEGIN
		SELECT 'Paygroup is not Configured'
	END
	        
	--EXEC('Select * FROM ['+@OrgTbl+']')        
	--EXEC('Select * FROM #VIEWEMPMST')
	        
	DECLARE @Org1 VARCHAR(MAX)
	SET @Org1 =''
	SET @Org1 = (SELECT ' AND (A.['+ITEMS+']=b.['+ITEMS+'_Description]) '
				 FROM ATTRIBUTE.TYPECONFIGURATION(NOLOCK) t
				 CROSS APPLY (SELECT ITEMS FROM DBO.SPLIT(t.AttributeBasedOnDesc,'|'))b
				 WHERE Applicable=1 AND ModuleCode='CompensationAndBenefits'
				 AND SubModuleCode='CompensationAndBenefits'
				 FOR XML PATH(''))
	
	---PRINT @Org1
	DROP TABLE IF EXISTS ##Temp2
	        
	SET @SqlString =''
	SET @SqlString ='SELECT B.Empcode,A.*  
					 INTO ##Temp2
					 FROM ['+@OrgTbl+'] A
					 INNER JOIN #VIEWEMPMST B ON 1=1 ' + @Org1 + ''
	        
	---PRINT @SqlString
	EXEC(@SqlString)
	        
	--SELECT * FROM ##Temp2
	DROP TABLE IF EXISTS #FinalData
	        
	SELECT	ED_EMPID AS EmployeeID,
			ISNULL(ED_EMPCode,'') AS EmployeeCode,
			GPP_CountryID AS CountryId,
			ISNULL(ED_Salutation,'') AS Salutation,
			ISNULL(ED_FirstName,'') AS FirstName,
			ISNULL(ED_MiddleName,'') AS MiddleName,
			ISNULL(ED_LastName,'') AS LastName,
			ED_DOB AS DOB,
			(CASE WHEN CONVERT(date, ED_DOJ) >= '2025-01-01' 
				  THEN CONVERT(date, ED_DOJ) 
				  ELSE '2025-01-01' 
			 END) AS DOJ,
			(CASE WHEN CONVERT(date, ED_DOJ) >= '2025-01-01' 
				  THEN CONVERT(date, ED_DOJ) 
				  ELSE '2025-01-01' 
			 END) AS FromDate,
			(CASE WHEN LOWER(ED_Sex) = 'male' THEN 1
				  WHEN LOWER(ED_Sex) = 'female' THEN 2 
				  ELSE 3 
			 END) AS Gender,
			P.PayheadCode,
	        P.PayHeadCategoryID,
	        PM_PayheadID AS PayheadId,
	        GPP_PropertyCode,
	        ISNULL(PropertyChecked,0) AS PropertyChecked,
	        ES.MonthlyRate,
	        ES.YearlyValue AS YearlyRate,
	        CONVERT(date, ES.ToDate) AS ToDate,
	        CONVERT(date, ES.EffDate) AS EffDate,
			LeenPayApp,
			TaxApp,
			SpotTaxApp,
			ES.updateDate AS updateDate_timestamp
	INTO #FinalData
	FROM ##Temp2 A
	INNER JOIN Common.GlobalPaygroupPayheadProperties (NOLOCK) B ON (A.PaygroupID = B.APGD_PaygroupID AND GPP_CountryID = 103)
	INNER JOIN dbo.Payheadmaster (NOLOCK) P ON B.PM_PayheadID = P.PayHeadID AND P.Applicable = 1
	INNER JOIN COMMON.ATTRIBUTEPAYGROUPPAYHEADDETAILS (NOLOCK) ATT ON ATT.PayHeadID = P.PayHeadID
	INNER JOIN dbo.EmpSALARY_MASTER ES ON A.Empcode = ES.EMpcode AND ES.Todate IS NULL AND P.PayHeadCode = ES.HeadCode
	INNER JOIN dbo.ReqRec_EmployeeDetails (NOLOCK) ED ON ES.EmpCode = ED.ED_EMPCode
	WHERE ES.MonthlyRate <> 0 
	AND ED_Status IN (1,10)
	AND ES.EmpCode IN (SELECT EmpCode FROM #ChangedEmp)
	                
	--SELECT * FROM #FinalData
	        
	DECLARE @cols NVARCHAR(MAX) = '';
	DECLARE @sql NVARCHAR(MAX) = '';
	        
	-- 1. GET DISTINCT PROPERTY CODES
	        
	SELECT @cols = STRING_AGG(QUOTENAME(GPP_PropertyCode), ',')
	FROM (SELECT DISTINCT GPP_PropertyCode FROM #FinalData) AS x;
	        
	SET  @cols =ELCM_CommonCore.ZTP.ProperCaseCSV(@cols);
	        
	---PRINT @cols
	        
	-- 2. BUILD DYNAMIC SQL
	
	SET @sql = 'SELECT		EmployeeID,EmployeeCode,CountryId,Salutation,FirstName,MiddleName,LastName,DOB,''2026-01-01'' AS DOJ,Gender,PayheadCode,PayHeadCategoryID,PayheadId,MonthlyRate,YearlyRate,''2026-01-01'' AS FromDate,ToDate,EffDate,LeenPayApp,TaxApp,SpotTaxApp,' + @cols + ',updateDate_timestamp
				FROM (	SELECT		EmployeeID,EmployeeCode,CountryId,Salutation,FirstName,MiddleName,LastName,DOB,DOJ,Gender,PayheadCode,PayHeadCategoryID ,PayheadId,MonthlyRate,YearlyRate,FromDate,ToDate,EffDate,LeenPayApp,TaxApp,SpotTaxApp,GPP_PropertyCode,ISNULL(CAST  (PropertyChecked AS INT),0) PropertyChecked, updateDate_timestamp
						FROM #FinalData
					 ) AS src
				PIVOT (
						MAX(PropertyChecked) FOR GPP_PropertyCode IN (' + @cols + ')
					  ) AS p';
	PRINT @sql

	-- 3. EXECUTE
	        
	EXEC sp_executesql @sql;

END