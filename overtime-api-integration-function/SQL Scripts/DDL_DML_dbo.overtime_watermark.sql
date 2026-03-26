
CREATE TABLE dbo.overtime_watermark 
(
	[ID] INT IDENTITY(1,1) PRIMARY KEY,
    [last_timestamp] DATETIME
);

--Insert Into dbo.overtime_watermark ([last_timestamp]) Values('2026-03-18 14:39:05.337')

