module.exports = (drop) => {
    return `
    SET NOCOUNT ON;

    DECLARE @ViewName VARCHAR(255);
    DECLARE @error_table TABLE
        (
        view_name VARCHAR(255) ,
        error_msg VARCHAR(MAX)
        );

    DECLARE view_cursor CURSOR FAST_FORWARD
    FOR
        --- Get all the user defined views with no schema binding on them
        SELECT DISTINCT
                '[' + ss.name + '].[' + av.name +']' AS ViewName
        FROM    sys.all_views av
                JOIN sys.schemas ss ON av.schema_id = ss.schema_id
        WHERE   OBJECTPROPERTY(av.[object_id], 'IsSchemaBound') <> 1
                AND av.Is_Ms_Shipped = 0

    OPEN view_cursor

    FETCH NEXT FROM view_cursor
    INTO @ViewName

    WHILE @@FETCH_STATUS = 0
        BEGIN

            BEGIN TRY
                -- Refresh the view
                EXEC sp_refreshview @ViewName;

                -- RAISERROR('%s', 10, 1, @ViewName) WITH NOWAIT;

            END TRY
            BEGIN CATCH
                IF @@trancount > 0 ROLLBACK TRANSACTION
                --- Insert all the errors
                IF (1=${drop ? 1 : 0})
                BEGIN
                    EXEC ('DROP VIEW ' + @ViewName)
                END ELSE
                BEGIN
                    INSERT INTO
                        @error_table(view_name, error_msg)
                    SELECT  @ViewName, ERROR_MESSAGE();
                END

            END CATCH

            FETCH NEXT FROM view_cursor INTO @ViewName;

        END

        --- Check if there was an error
    IF EXISTS (SELECT TOP 1 1 FROM @error_table)
        BEGIN
            SELECT  view_name ,
                    error_msg
            FROM    @error_table;
        END

    CLOSE view_cursor
    DEALLOCATE view_cursor

    SET NOCOUNT OFF;`;
};
