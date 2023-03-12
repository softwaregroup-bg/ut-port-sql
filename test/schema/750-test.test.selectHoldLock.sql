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
    ;
    WITH test(col1, col2) AS (
        SELECT '1', '2'
    ) SELECT * FROM test
END TRY
BEGIN CATCH
    IF (@@trancount > 0)
        ROLLBACK TRANSACTION;
            THROW;
    SELECT '1'
    WHERE 1 = 1
END CATCH
