ALTER PROCEDURE test.[test.selectHoldLock]
    @reverse BIT = 0
AS
BEGIN TRY
    BEGIN TRANSACTION
        SELECT * FROM test.test WITH (UPDLOCK, HOLDLOCK) WHERE id = IIF(@reverse = 0, 1, 2)
        WAITFOR DELAY '00:00:02'
        SELECT * FROM test.test WITH (UPDLOCK, HOLDLOCK) WHERE id = IIF(@reverse = 0, 2, 1)
        WAITFOR DELAY '00:00:02'
    COMMIT TRANSACTION
END TRY
BEGIN CATCH
    IF (@@trancount > 0)
        ROLLBACK TRANSACTION;
            THROW;
END CATCH
