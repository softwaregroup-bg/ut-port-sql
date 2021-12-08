ALTER PROCEDURE test.[test.update]
    @makeDeadlock BIT
AS
SET NOCOUNT ON
IF (ISNULL(@makeDeadlock, 0) = 0)
BEGIN
    BEGIN TRAN ordered
        UPDATE test.test1 SET id = id + 1;
        UPDATE test.test2 SET id = id + 1;
END
ELSE
BEGIN
    BEGIN TRAN reversed
        UPDATE test.test2 SET id = id + 1;
        UPDATE test.test1 SET id = id + 1;
END
