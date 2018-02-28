module.exports = (binding) => {
    let columns = binding.fields.filter(f => !f.identity).map(f => `[${f.column}]`).join(', ');
    return `
    CREATE PROCEDURE ${binding.spName}
        @data ${binding.tt} READONLY
    AS
    SET NOCOUNT ON
    DECLARE @result ${binding.tt}
    BEGIN TRY
        INSERT INTO ${binding.name} (${columns})
        OUTPUT INSERTED.* INTO @result
        SELECT ${columns}
        FROM @data

        SELECT 'data' AS resultSetName
        SELECT * from @result
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT != 0
            ROLLBACK TRANSACTION
        DECLARE
            @errmsg NVARCHAR(2048),
            @severity TINYINT,
            @state TINYINT,
            @errno INT,
            @proc sysname,
            @lineno INT
        SELECT
            @errmsg = error_message(),
            @severity = error_severity(),
            @state = error_state(),
            @errno = error_number(),
            @proc = error_procedure(),
            @lineno = error_line()
        IF @errmsg NOT LIKE '***%'
        BEGIN
            SELECT @errmsg = '*** ' + COALESCE(QUOTENAME(@proc), '<dynamic SQL>') +
                ', Line ' + LTRIM(STR(@lineno)) + '. Errno ' +
                LTRIM(STR(@errno)) + ': ' + @errmsg
        END
        RAISERROR('%s', @severity, @state, @errmsg)
        RETURN 55555
    END CATCH
    `;
};
