ALTER PROCEDURE test.[test.merge]
    @data test.customTT READONLY,
    /*{
        "data.content": 1
    }*/
    @ngram test.ngramTT READONLY -- (ngram for encrypted columns)
AS

DECLARE @ngramIndex test.ngramIndexTT
DECLARE @records test.testTT

MERGE INTO test.test AS target
USING (
    SELECT
        ROW_NUMBER() OVER(ORDER BY content) AS id,
        content AS txt
    FROM
        @data
) AS source ON target.id = source.id
WHEN NOT MATCHED BY target THEN
    INSERT (id, txt)
    VALUES (source.id, source.txt)
WHEN MATCHED AND target.txt != source.txt THEN
    UPDATE SET target.txt = source.txt
OUTPUT INSERTED.id, INSERTED.txt INTO @records(id, txt);

INSERT INTO @ngramIndex(id, field, ngram)
SELECT r.id, n.field, n.ngram
FROM (
    SELECT id, RANK() OVER (ORDER BY txt) [row]
    FROM @records
) r
JOIN @ngram n ON n.[row] = r.[row] AND n.param = 'data'

EXEC [test].[testIndexMerge] @ngramIndex = @ngramIndex
