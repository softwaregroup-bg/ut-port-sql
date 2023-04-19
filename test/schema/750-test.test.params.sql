ALTER PROCEDURE test.[test.params]
    @obj VARBINARY(128),
    @test INT,
    @tt test.customTT READONLY,
    @meta core.metaDataTT READONLY
AS
DECLARE @callParams XML

BEGIN TRY
    --ut-permission-check

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
END TRY
BEGIN CATCH
    EXEC [core].[error]
    RETURN 55555
END CATCH
