
CREATE TABLE dbo.variable_watermark 
(
	[ID] INT IDENTITY(1,1) PRIMARY KEY,
    [last_timestamp] DATETIME,
	[source_db] VARCHAR(50)
);

--Insert Into dbo.variable_watermark ([last_timestamp],[source_db]) Values('2026-06-16 16:23:41.230','ELCM_SBFCP3')

