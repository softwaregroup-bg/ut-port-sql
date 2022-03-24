ALTER PROCEDURE test.[test.words]
    @words [test].[wordTT] READONLY
AS
SELECT * FROM @words
