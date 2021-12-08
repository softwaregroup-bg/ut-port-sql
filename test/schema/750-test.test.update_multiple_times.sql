ALTER PROCEDURE test.[test.update_multiple_times]
    @value1 int,
    @value2 int
AS
BEGIN TRY

    BEGIN TRAN purpose
        SELECT * FROM test.test1 WITH (UPDLOCK, HOLDLOCK) WHERE id = @value1
        WAITFOR DELAY '00:00:03'
        SELECT * FROM test.test1 WITH (UPDLOCK, HOLDLOCK) WHERE id = @value2
        WAITFOR DELAY '00:00:03'

COMMIT TRANSACTION

END TRY
BEGIN CATCH
    IF @@trancount > 0
        ROLLBACK TRANSACTION;

            THROW -- a linting error put this here...
END CATCH
