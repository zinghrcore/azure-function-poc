
CREATE TABLE dbo.overtime_watermark
(
	[ID] INT IDENTITY(1,1) PRIMARY KEY,
    [last_timestamp] DATETIME,
	[source_db] VARCHAR(50)
);

--Insert Into dbo.overtime_watermark ([last_timestamp],[source_db]) Values('2026-03-18 14:39:05.337','ELCM_FBINCQA5')

