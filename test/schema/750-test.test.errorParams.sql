ALTER PROCEDURE test.[test.errorParams]
AS
BEGIN TRY
    DECLARE @errorMessage VARCHAR(255) = 'test.errorParams --foo=bar -n5' + CHAR(10) + '--baz=etc'
    RAISERROR(@errorMessage, 16, 1);
END TRY
BEGIN CATCH
    EXEC [core].[error]
    RETURN 55555
END CATCH
