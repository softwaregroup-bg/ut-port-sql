module.exports = {
    loadSchema: function() {
        return `
        SELECT
            o.create_date,
            c.id,
            c.colid,
            RTRIM(o.[type]) [type],
            SCHEMA_NAME(o.schema_id) [namespace],
            o.Name AS [name],
            SCHEMA_NAME(o.schema_id) + '.' + o.Name AS [full],
            CASE o.[type]
                WHEN 'SN' THEN 'DROP SYNONYM [' + SCHEMA_NAME(o.schema_id) + '].[' + o.Name +
                                '] CREATE SYNONYM [' + SCHEMA_NAME(o.schema_id) + '].[' + o.Name + '] FOR ' +  s.base_object_name
                ELSE c.text
            END AS [source]

        FROM
            sys.objects o
        LEFT JOIN
            dbo.syscomments c on o.object_id = c.id
        LEFT JOIN
            sys.synonyms s on s.object_id = o.object_id
        WHERE
            o.type IN ('V', 'P', 'FN','F','IF','SN','TF','TR','U') AND
            user_name(objectproperty(o.object_id, 'OwnerId')) in (USER_NAME(),'dbo') AND
            objectproperty(o.object_id, 'IsMSShipped') = 0
        UNION ALL
        SELECT 0,0,0,'S',name,NULL,NULL,NULL FROM sys.schemas WHERE principal_id = USER_ID()
        UNION ALL
        SELECT
            0,0,0,'T',SCHEMA_NAME(t.schema_id)+'.'+t.name,NULL,NULL,NULL
        FROM
            sys.types t
        JOIN
            sys.schemas s ON s.principal_id = USER_ID() AND s.schema_id=t.schema_id
        WHERE
            t.is_user_defined=1
        ORDER BY
            1, 2, 3

        SELECT
            SCHEMA_NAME(types.schema_id) + '.' + types.name name,
            c.name [column],
            st.name type,
            CASE
                WHEN st.name in ('decimal','numeric') then CAST(c.[precision] AS VARCHAR)
                WHEN st.name in ('datetime2','time','datetimeoffset') then CAST(c.[scale] AS VARCHAR)
                WHEN st.name in ('varchar','varbinary') AND c.max_length>=0 THEN CAST(c.max_length as VARCHAR)
                WHEN st.name in ('nvarchar','nvarbinary') AND c.max_length>=0 THEN CAST(c.max_length/2 as VARCHAR)
                WHEN st.name in ('varchar','nvarchar','varbinary','nvarbinary') AND c.max_length<0 THEN 'max'
            END [length],
            CASE
                WHEN st.name in ('decimal','numeric') then c.scale
            END scale,
            object_definition(c.default_object_id) [default]
        FROM
            sys.table_types types
        JOIN
            sys.columns c ON types.type_table_object_id = c.object_id
        JOIN
            sys.systypes AS st ON st.xtype = c.system_type_id
        WHERE
            types.is_user_defined = 1 AND st.name <> 'sysname'
        ORDER BY
            1,c.column_id

        SELECT
            1 sort,
            s.name + '.' + o.name [name],
            'IF (OBJECT_ID(''[' + s.name + '].[' + o.name + ']'') IS NOT NULL) DROP PROCEDURE [' + s.name + '].[' + o.name + ']' [drop],
            p.name [param],
            SCHEMA_NAME(t.schema_id) + '.' + t.name [type]
        FROM
            sys.schemas s
        JOIN
            sys.objects o ON o.schema_id = s.schema_id
        JOIN
            sys.parameters p ON p.object_id = o.object_id
        JOIN
            sys.types t ON p.user_type_id = t.user_type_id AND t.is_user_defined=1
        WHERE
            user_name(objectproperty(o.object_id, 'OwnerId')) in (USER_NAME(),'dbo')
        UNION
        SELECT
            2,
            s.name + '.' + t.name [name],
            'DROP TYPE [' + s.name + '].[' + t.name + ']' [drop],
            NULL [param],
            SCHEMA_NAME(t.schema_id) + '.' + t.name [type]
        FROM
            sys.schemas s
        JOIN
            sys.types t ON t.schema_id=s.schema_id and t.is_user_defined=1
        WHERE
            user_name(s.principal_id) in (USER_NAME(),'dbo')
        ORDER BY 1`;
    },
    refreshView: function(drop) {
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

                    RAISERROR('%s', 10, 1, @ViewName) WITH NOWAIT;

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
    },
    auditLog: function(statement) {
        if (!statement.params) {
            return;
        }
        var sql = '    DECLARE @callParams XML = ( SELECT ';
        statement.params.map(function(param) {
            if (param.def.type === 'table') {
                sql += `(SELECT * from @${param.name} rows FOR XML AUTO, TYPE) ${param.name}, `;
            } else {
                sql += `@${param.name} ${param.name}, `;
            }
        });
        sql = sql.replace(/,\s$/, ' ');
        sql += 'FOR XML RAW(\'params\'),TYPE) EXEC core.auditCall @procid = @@PROCID, @params=@callParams';
        return sql;
    },
    callParams: function(statement) {
        if (!statement.params) {
            return;
        }
        var sql = 'DECLARE @callParams XML = ( SELECT ';
        statement.params.map(function(param) {
            if (param.def.type === 'table') {
                sql += `(SELECT * from @${param.name} rows FOR XML AUTO, TYPE) ${param.name}, `;
            } else {
                sql += `@${param.name} ${param.name}, `;
            }
        });
        sql = sql.replace(/,\s$/, ' ');
        sql += 'FOR XML RAW(\'params\'),TYPE)';
        return sql;
    },
    createDatabase: function(name, user) {
        return `
        IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = '${name}')
        BEGIN
          CREATE DATABASE [${name}]
          ALTER DATABASE [${name}] SET READ_COMMITTED_SNAPSHOT ON
          ALTER DATABASE [${name}] SET AUTO_SHRINK OFF
        END`;
    },
    createUser: function(name, user, password) {
        return `
        IF NOT EXISTS (SELECT name FROM master.sys.server_principals WHERE name = '${user}')
        BEGIN
            CREATE LOGIN [${user}] WITH PASSWORD = N'${password}', CHECK_POLICY = OFF
        END
        USE [${name}]
        IF NOT EXISTS (SELECT name FROM sys.database_principals WHERE name = '${user}')
        BEGIN
            CREATE USER [${user}] FOR LOGIN [${user}]
        END
        EXEC sp_addrolemember 'db_owner', '${user}'`;
    }
};
