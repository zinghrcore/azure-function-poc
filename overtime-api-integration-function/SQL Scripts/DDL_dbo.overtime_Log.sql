
CREATE TABLE dbo.overtime_Log 
(
    [Timestamp] DATETIME,
    [BatchSize] INT,
    [Status] VARCHAR(50),
    [Details] VARCHAR(MAX),
    [IsBatchComplete] BIT DEFAULT 0,
	[Payload] VARCHAR(MAX)
);