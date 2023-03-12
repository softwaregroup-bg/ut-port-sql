ALTER PROCEDURE test.[test.error]
AS
BEGIN TRY
    RAISERROR('test.error', 16, 1);
END TRY
BEGIN CATCH
    EXEC [core].[error]
    RETURN 55555
END CATCH
