ALTER PROCEDURE test.[test.errorParams]
AS
BEGIN TRY
    RAISERROR('test.errorParams --foo=bar -n5', 16, 1);
END TRY
BEGIN CATCH
    EXEC [core].[error]
    RETURN 55555
END CATCH
