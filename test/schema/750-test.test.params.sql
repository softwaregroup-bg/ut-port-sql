ALTER PROCEDURE test.[test.params]
    @obj VARBINARY(MAX),
    @tt test.testTT READONLY
AS
SELECT 'obj' AS resultSetName, 1 AS single
SELECT @obj obj

SELECT 'tt' AS resultSetName
SELECT * FROM @tt
