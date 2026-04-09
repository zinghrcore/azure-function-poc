
CREATE TABLE dbo.Attendance_Watermark
(
	[ID] INT IDENTITY(1,1) PRIMARY KEY,
    [last_timestamp] DATETIME,
	[source_db] VARCHAR(50)
);

--Insert Into dbo.Attendance_watermark ([last_timestamp],[source_db]) Values('2026-04-02 18:32:19.300','ELCM_BURGERKINGGROWTH')