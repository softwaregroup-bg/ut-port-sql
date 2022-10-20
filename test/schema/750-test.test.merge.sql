ALTER PROCEDURE test.[test.merge]
    @data test.testTT READONLY,
    /*{
        "data.txt": 1
    }*/
    @ngram test.ngramTT READONLY -- (ngram for encrypted columns)
AS

DECLARE @ngramIndex test.ngramIndexTT
DECLARE @inserted test.testTT

MERGE INTO test.test AS target
USING (SELECT id, txt FROM @data) AS source
    ON target.id = source.id
WHEN NOT MATCHED BY target THEN
    INSERT (id, txt)
    VALUES (source.id, source.txt)
OUTPUT INSERTED.* INTO @inserted;

INSERT INTO @ngramIndex(id, field, ngram)
SELECT r.id, n.field, n.ngram
FROM (
    SELECT id, RANK() OVER (ORDER BY id) [row]
    FROM @inserted
) r
JOIN @ngram n ON n.[row] = r.[row] AND n.param = 'data'

EXEC [test].[testIndexMerge] @ngramIndex = @ngramIndex
