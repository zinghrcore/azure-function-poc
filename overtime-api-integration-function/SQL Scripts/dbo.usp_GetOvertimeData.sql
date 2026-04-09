
--EXEC dbo.usp_GetOvertimeData @LastTimestamp = '2026-03-18 14:39:05.337'

CREATE OR ALTER PROCEDURE dbo.usp_GetOvertimeData
    @LastTimestamp DATETIME = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -----------------------------------------
    -- SINGLE PASS QUERY (NO TEMP TABLE)
    -----------------------------------------

    SELECT	ED.ED_EMPID AS EmployeeCode
			,RZ.[Date]
			,JSON_VALUE(value.value, '$.ParameterCode') AS Code
			,ROUND(CAST(JSON_VALUE(value.value, '$.ParameterValue') AS FLOAT) / 60.0, 2) AS ExtraHrs
			,CASE 
			    WHEN JSON_VALUE(value.value, '$.Conversion') = 'Hours' THEN 'H'
			    ELSE JSON_VALUE(value.value, '$.Conversion')
			END AS Conversion
			,pm.Rate AS ExtraTimePay
			,RZ.OT_UpdatedDate AS updateDate_timestamp

    FROM TNA.Rostering_Z2 RZ WITH (NOLOCK)
	INNER JOIN dbo.ReqRec_EmployeeDetails ED ON RZ.EmpCode = ED.ED_EMPCode
	CROSS APPLY OPENJSON(RZ.OT_Param_json) AS value
	INNER JOIN TNA.OT_ParameterMaster pm ON JSON_VALUE(value.value, '$.ParameterCode') = pm.Name

    WHERE RZ.OT_Param_json IS NOT NULL
    AND (
        (@LastTimestamp IS NOT NULL AND RZ.OT_UpdatedDate > @LastTimestamp)
        OR
        (@LastTimestamp IS NULL AND RZ.[Date] >= DATEADD(DAY, -90, GETDATE()))
    )

    ORDER BY ED.ED_EMPID, RZ.[Date];

END