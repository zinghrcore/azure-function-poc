
CREATE PROCEDURE dbo.uspGetAttendanceData
@LastTimestamp DATETIME = NULL
AS
BEGIN

SET NOCOUNT ON;

    SELECT
           ED.ED_EMPID AS EmployeeID,
           EV.EmpCode AS EmployeeCode,
           EV.[Date] AS AttendanceDate,
           EV.[Date] AS ApprovedDate,
           CASE
               WHEN EV.AttValue1 = -1 THEN 0
               WHEN EV.AttValue1 IS NULL THEN 0
               ELSE EV.AttValue1
           END AS Attendance,
           0 AS EarlyMinutes,
           0 AS LateMinutes,
           EV.UpdatedDate AS updateDate_timestamp
       
	FROM 
		TNA.Rostering_Z2 EV WITH (NOLOCK)
		INNER JOIN dbo.ReqRec_EmployeeDetails ED WITH (NOLOCK) ON EV.EmpCode = ED.ED_EMPCode
	
	WHERE 
		(@LastTimestamp IS NULL AND EV.Date >= DATEADD(day, -90, GETDATE()))
			OR
		(@LastTimestamp IS NOT NULL AND EV.UpdatedDate > @LastTimestamp)
       
	ORDER BY
		ED.ED_EMPID
		,EV.[Date]
		,EV.UpdatedDate
END