
CREATE TABLE dbo.overtime_watermark
(
	[ID] INT IDENTITY(1,1) PRIMARY KEY,
    [last_timestamp] DATETIME,
	[source_db] VARCHAR(50)
);

--INSERT INTO dbo.overtime_watermark ([last_timestamp],[source_db]) VALUES (NULL,'ELCM_SBFCP3')

