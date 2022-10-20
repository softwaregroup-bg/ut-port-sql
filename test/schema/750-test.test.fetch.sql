ALTER PROCEDURE test.[test.fetch]
    /*{
        "data.txt": 1
    }*/
    @ngramTest [test].[ngramTT] READONLY
AS

SELECT 'data' AS resultSetName

SELECT
    id, txt
FROM
    test.test
WHERE
    id IN (SELECT id FROM test.testSearch(@ngramTest, 1))
ORDER BY id
