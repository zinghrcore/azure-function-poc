
CREATE TABLE dbo.overtime_watermark
(
	[ID] INT IDENTITY(1,1) PRIMARY KEY,
    [last_timestamp] DATETIME,
	[source_db] VARCHAR(50)
);

--INSERT INTO dbo.overtime_watermark ([last_timestamp],[source_db]) VALUES ('2026-04-14 15:33:45.050','ELCM_FBINCQA5')

