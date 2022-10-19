ALTER PROCEDURE test.[test.fetch]
    /*{
        "data.content": 1
    }*/
    @ngramTest [test].[ngramTT] READONLY
AS

SELECT 'data' AS resultSetName

SELECT
    t.id, t.txt
FROM
    test.test t
JOIN
    test.testSearch(@ngramTest, 1) s
ON
    s.id = t.id
