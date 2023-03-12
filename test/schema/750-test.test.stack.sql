ALTER PROCEDURE test.[test.stack]
AS
BEGIN TRY
    SELECT 1 result
    EXEC [test].[test.error]
END TRY
BEGIN CATCH
    EXEC [core].[error]
    RETURN 55555
END CATCH
