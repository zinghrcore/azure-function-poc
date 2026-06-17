
--DROP TABLE dbo.salaryrevision_Log 

CREATE TABLE dbo.salaryrevision_Log 
(
	[ID] INT IDENTITY(1,1),
    [Timestamp] DATETIME,
    [BatchSize] INT,
    [Status] VARCHAR(50),
    [Details] VARCHAR(MAX),
    IsBatchComplete BIT DEFAULT 0,
	[Payload] VARCHAR(MAX) NULL
);