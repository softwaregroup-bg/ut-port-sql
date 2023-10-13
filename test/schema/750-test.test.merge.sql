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
    INSERT (id, txt, column1, column3, column4)
    VALUES (source.id, source.txt, '.', 0, 0)
OUTPUT INSERTED.id, INSERTED.txt INTO @inserted(id, txt);

INSERT INTO @ngramIndex(id, field, ngram)
SELECT r.id, n.field, n.ngram
FROM (
    SELECT id, RANK() OVER (ORDER BY id) [row]
    FROM @inserted
) r
JOIN @ngram n ON n.[row] = r.[row] AND n.param = 'data'

EXEC [test].[testIndexMerge] @ngramIndex = @ngramIndex
