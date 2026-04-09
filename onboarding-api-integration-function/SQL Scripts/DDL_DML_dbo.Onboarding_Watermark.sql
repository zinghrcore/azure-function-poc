
CREATE TABLE dbo.onboarding_watermark 
(
	[ID] INT IDENTITY(1,1) PRIMARY KEY,
    [last_timestamp] DATETIME,
	[source_db] VARCHAR(50)
);

--Insert Into dbo.onboarding_watermark ([last_timestamp],[source_db]) Values('2026-03-25 12:05:26.973','ELCM_FBINCQA5')

