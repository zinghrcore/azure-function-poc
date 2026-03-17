
CREATE TABLE dbo.Attendance_Log 
(
    [Timestamp] DATETIME,
    [BatchSize] INT,
    [EmpCodes] VARCHAR(MAX),
    [Status] VARCHAR(50),
    [Details] VARCHAR(MAX),
    IsBatchComplete BIT DEFAULT 0
);

--Insert into dbo.Attendance_Watermark(last_timestamp) values ('2026-03-13 17:06:30.133')
