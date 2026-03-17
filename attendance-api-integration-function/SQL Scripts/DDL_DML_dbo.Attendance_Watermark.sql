
CREATE TABLE dbo.Attendance_Watermark
(
	[ID] INT IDENTITY(1,1) PRIMARY KEY,
    [last_timestamp] DATETIME
);

--INSERT INTO dbo.Attendance_Watermark(last_timestamp) VALUES ('2026-03-12 16:43:40.813');
