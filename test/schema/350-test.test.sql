CREATE TABLE [test].[test] (
    id TINYINT,
    txt VARBINARY(48),
    [column1] VARCHAR(30) NOT NULL,
    [column2] VARCHAR(30),
    [column3] DECIMAL(10, 5) NOT NULL,
    [column4] INT NOT NULL,
    [column5] INT,
    CONSTRAINT pkTestTest_Id PRIMARY KEY CLUSTERED (id)
)
/*{
    "ngram": {
        "index": true,
        "search": true
    }
}*/
