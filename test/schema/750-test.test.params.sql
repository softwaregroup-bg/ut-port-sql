ALTER PROCEDURE test.[test.params]
    @obj VARBINARY(128),
    @test INT,
    @tt test.customTT READONLY
AS
DECLARE @callParams XML

SELECT 'obj' AS resultSetName, 1 AS single
SELECT @obj obj

SELECT 'tt' AS resultSetName
SELECT * FROM @tt

IF @test = 1
BEGIN
    SELECT 'test-no-coverage' resultSetName
    SELECT @test test
END ELSE
BEGIN
    SELECT 'test-coverage' resultSetName
    SELECT @test test
    UNION ALL
    SELECT @test test
END
