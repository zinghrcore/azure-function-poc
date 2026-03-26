
--EXEC dbo.usp_GetOvertimeData @LastTimestamp = '2026-03-25 12:05:26.973'

CREATE OR ALTER PROCEDURE dbo.usp_GetOvertimeData
    @LastTimestamp DATETIME = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -----------------------------------------
    -- STEP 1: GET CHANGED RECORDS (CDC)
    -----------------------------------------

    IF OBJECT_ID('tempdb..#ChangedRecords') IS NOT NULL DROP TABLE #ChangedRecords;

    SELECT DISTINCT
        ED.ED_EMPID AS EmployeeCode,
        RZ.Date,
        RZ.OT_UpdatedDate AS updateDate_timestamp
    INTO #ChangedRecords
    FROM TNA.Rostering_Z2 RZ WITH (NOLOCK)
    INNER JOIN dbo.ReqRec_EmployeeDetails ED ON RZ.EmpCode = ED.ED_EMPCode
    WHERE 
        RZ.OT_Param_json IS NOT NULL
        AND (
            (@LastTimestamp IS NOT NULL AND RZ.OT_UpdatedDate > @LastTimestamp)
            OR
            (@LastTimestamp IS NULL AND RZ.Date >= DATEADD(DAY, -90, GETDATE())));

    -----------------------------------------
    -- STEP 2: EXIT IF NO DATA
    -----------------------------------------

    IF NOT EXISTS (SELECT 1 FROM #ChangedRecords)
    BEGIN
        SELECT TOP 0
            CAST(NULL AS VARCHAR(50)) AS EmployeeCode,
            CAST(NULL AS DATE) AS [Date],
            CAST(NULL AS VARCHAR(50)) AS Code,
            CAST(NULL AS FLOAT) AS ExtraHrs,
            CAST(NULL AS VARCHAR(10)) AS Conversion,
            CAST(NULL AS FLOAT) AS ExtraTimePay,
            CAST(NULL AS DATETIME) AS updateDate_timestamp;
        RETURN;
    END

    -----------------------------------------
    -- STEP 3: MAIN DATA FETCH
    -----------------------------------------

    IF OBJECT_ID('tempdb..#FinalData') IS NOT NULL DROP TABLE #FinalData;

    SELECT
        rz.EmployeeCode,
        rz.Date,
        rz.Code,
        ROUND((rz.ExtraHrs / 60.0), 2) AS ExtraHrs,
        CASE 
            WHEN rz.Conversion = 'Hours' THEN 'H'
            ELSE rz.Conversion
        END AS Conversion,
        pm.Rate AS ExtraTimePay,
        rz.updateDate_timestamp
    INTO #FinalData
    FROM (
        SELECT
            ED.ED_EMPID AS EmployeeCode,
            RZ.[Date],
            JSON_VALUE(value.value, '$.ParameterCode') AS Code,
            CAST(JSON_VALUE(value.value, '$.ParameterValue') AS FLOAT) AS ExtraHrs,
            JSON_VALUE(value.value, '$.Conversion') AS Conversion,
            RZ.OT_UpdatedDate AS updateDate_timestamp
        FROM TNA.Rostering_Z2 RZ WITH (NOLOCK)
        INNER JOIN dbo.ReqRec_EmployeeDetails ED 
            ON RZ.EmpCode = ED.ED_EMPCode
        CROSS APPLY OPENJSON(RZ.OT_Param_json) AS value

        INNER JOIN #ChangedRecords CR
            ON CR.EmployeeCode = ED.ED_EMPID
            AND CR.Date = RZ.Date

    ) rz
    INNER JOIN TNA.OT_ParameterMaster pm 
        ON rz.Code = pm.Name;

    -----------------------------------------
    -- STEP 4: FINAL OUTPUT (SINGLE RESULT)
    -----------------------------------------

    SELECT 
        EmployeeCode,
        [Date],
        Code,
        ExtraHrs,
        Conversion,
        ExtraTimePay,
        updateDate_timestamp
    FROM #FinalData
    ORDER BY EmployeeCode, Date;

END