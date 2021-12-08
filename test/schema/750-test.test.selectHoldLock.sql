ALTER PROCEDURE test.[test.selectHoldLock]
    @reversed BIT = 0
AS
DECLARE @reminder TINYINT = CAST(@reversed AS TINYINT)
BEGIN TRY
    BEGIN TRANSACTION
        SELECT * FROM test.test WITH (UPDLOCK, HOLDLOCK) WHERE id % 2 != @reminder
        WAITFOR DELAY '00:00:03'
        SELECT * FROM test.test WITH (UPDLOCK, HOLDLOCK) WHERE id % 2 = @reminder
        WAITFOR DELAY '00:00:03'
    COMMIT TRANSACTION
END TRY
BEGIN CATCH
    IF (@@trancount > 0)
        ROLLBACK TRANSACTION;
            THROW;
END CATCH
