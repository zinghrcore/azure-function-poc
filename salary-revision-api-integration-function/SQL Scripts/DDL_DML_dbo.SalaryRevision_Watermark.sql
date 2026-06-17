
CREATE TABLE dbo.salaryrevision_watermark
(
	[ID] INT IDENTITY(1,1) PRIMARY KEY,
    [last_timestamp] DATETIME,
	[source_db] VARCHAR(50)
);

--Insert Into dbo.salaryrevision_watermark ([last_timestamp],[source_db]) Values(NULL,'ELCM_SBFCP2')

