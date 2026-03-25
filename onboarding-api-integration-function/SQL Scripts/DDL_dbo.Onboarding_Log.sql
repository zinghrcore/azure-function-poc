
CREATE TABLE dbo.Onboarding_Log 
(
    [Timestamp] DATETIME,
    [BatchSize] INT,
    [EmpCodes] VARCHAR(MAX),
    [Status] VARCHAR(50),
    [Details] VARCHAR(MAX),
    IsBatchComplete BIT DEFAULT 0
);