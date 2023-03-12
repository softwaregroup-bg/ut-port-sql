ALTER PROCEDURE [core].[errorStack]
    @procId INT = NULL,
    @dbId SMALLINT = NULL,
    @file NVARCHAR(255) = NULL,
    @fileLine INT = NULL,
    @params XML = NULL,
    @type VARCHAR(200) = NULL,
    @useRethrow BIT = 0,
    @errorNumber INT = 0
AS
SET @dbId = ISNULL(@dbId, DB_ID());
DECLARE @raise BIT = CASE WHEN @type IS NULL THEN 1 ELSE 0 END
IF @type IS NULL SET @type = error_message()
DECLARE
    @errmsg NVARCHAR(2048),
    @severity TINYINT,
    @state TINYINT,
    @errorMessage VARCHAR(250)

SELECT
    @severity = ISNULL(error_severity(), 16),
    @state = ISNULL(error_state(), 1),
    @errmsg = @type + CHAR(10) + '    at ' + ISNULL(OBJECT_SCHEMA_NAME(@procId, @dbId) + '.' + OBJECT_NAME(@procId, @dbId), '<SQL>') +
    ' (' + @file + ':' +
    LTRIM(STR(
        CASE
            ISNULL(error_procedure(), 'core.errorStack')
            WHEN 'core.errorStack' THEN @fileLine
            WHEN 'errorStack' THEN @fileLine
            ELSE ISNULL(error_line(), 1)
        END
    )) +
    ':1) errno:' +
    LTRIM(STR(ISNULL(error_number(), 0)))

SET @errorMessage = LEFT(@errmsg, 250)

IF @errorNumber > 100000
BEGIN
    IF @raise = 1 THROW @errorNumber, @errmsg, @state
    RETURN 55555
END
IF @useRethrow = 0
BEGIN
    IF @raise = 1
        RAISERROR('%s', @severity, @state, @errmsg)
    RETURN 55555
END
