CREATE TABLE [test].[test] (
    id TINYINT,
    txt VARBINARY(48),
    [column1] VARCHAR(40) NOT NULL, -- increased size
    [column2] VARCHAR(20), -- decreased size
    [add1] VARCHAR(20) NOT NULL DEFAULT('-'), -- new varchar column
    [add2] DECIMAL(10, 5), -- new decimal column
    CONSTRAINT pkTestTest_Id PRIMARY KEY CLUSTERED (id)
)
