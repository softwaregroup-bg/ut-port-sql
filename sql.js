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
                WHEN st.name in ('varchar','varbinary','char','binary') AND c.max_length>=0 THEN CAST(c.max_length as VARCHAR)
                WHEN st.name in ('nvarchar','nchar') AND c.max_length>=0 THEN CAST(c.max_length/2 as VARCHAR)
                WHEN st.name in ('varchar','varbinary','char','binary','nvarchar','nchar') AND c.max_length<0 THEN 'max'
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
            'IF (OBJECT_ID(''[' + s.name + '].[' + o.name + ']'') IS NOT NULL) DROP ' + CASE o.type WHEN 'FN' THEN 'FUNCTION' ELSE 'PROCEDURE' END + ' [' + s.name + '].[' + o.name + ']' [drop],
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
    },
    auditLog: function(statement) {
        if (!statement.params) {
            return;
        }
        var sql = '    DECLARE @callParams XML = ( SELECT ';
        statement.params.map(function(param) {
            if (param.def.type === 'table') {
                sql += `(SELECT * from @${param.name} rows FOR XML AUTO, TYPE) [${param.name}], `;
            } else {
                sql += `@${param.name} [${param.name}], `;
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
                sql += `(SELECT * from @${param.name} rows FOR XML AUTO, TYPE) [${param.name}], `;
            } else {
                sql += `@${param.name} [${param.name}], `;
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
        EXEC sp_addrolemember 'db_owner', '${user}'
        USE [master]
        GRANT VIEW SERVER STATE to [${user}]`;
    },
    enableDatabaseDiagrams: function(name) {
        return `
        USE [${name}]

        IF OBJECT_ID(N'dbo.sp_upgraddiagrams') IS NULL and IS_MEMBER('db_owner') = 1
        BEGIN
            EXEC sp_executesql N'
            CREATE PROCEDURE dbo.sp_upgraddiagrams
            AS
            BEGIN
                IF OBJECT_ID(N''dbo.sysdiagrams'') IS NOT NULL
                    return 0;
            
                CREATE TABLE dbo.sysdiagrams
                (
                    name sysname NOT NULL,
                    principal_id int NOT NULL,  -- we may change it to varbinary(85)
                    diagram_id int PRIMARY KEY IDENTITY,
                    version int,
            
                    definition varbinary(max)
                    CONSTRAINT UK_principal_name UNIQUE
                    (
                        principal_id,
                        name
                    )
                );

                /* Add this if we need to have some form of extended properties for diagrams */
                IF OBJECT_ID(N''dbo.sysdiagram_properties'') IS NULL
                BEGIN
                    CREATE TABLE dbo.sysdiagram_properties
                    (
                        diagram_id int,
                        name sysname,
                        value varbinary(max) NOT NULL
                    )
                END

                IF OBJECT_ID(N''dbo.dtproperties'') IS NOT NULL
                begin
                    insert into dbo.sysdiagrams
                    (
                        [name],
                        [principal_id],
                        [version],
                        [definition]
                    )
                    select   
                        convert(sysname, dgnm.[uvalue]),
                        DATABASE_PRINCIPAL_ID(N''dbo''),            -- will change to the sid of sa
                        0,                          -- zero for old format, dgdef.[version],
                        dgdef.[lvalue]
                    from dbo.[dtproperties] dgnm
                        inner join dbo.[dtproperties] dggd on dggd.[property] = ''DtgSchemaGUID'' and dggd.[objectid] = dgnm.[objectid] 
                        inner join dbo.[dtproperties] dgdef on dgdef.[property] = ''DtgSchemaDATA'' and dgdef.[objectid] = dgnm.[objectid]
                        
                    where dgnm.[property] = ''DtgSchemaNAME'' and dggd.[uvalue] like N''_EA3E6268-D998-11CE-9454-00AA00A3F36E_'' 
                    return 2;
                end
                return 1;
            END
            '
        END

        -- This sproc could be executed by any other users than dbo
        IF IS_MEMBER('db_owner') = 1
            EXEC dbo.sp_upgraddiagrams;

        IF OBJECT_ID(N'dbo.sp_helpdiagrams') IS NULL and IS_MEMBER('db_owner') = 1
        BEGIN
            EXEC sp_executesql N'
            CREATE PROCEDURE dbo.sp_helpdiagrams
            (
                @diagramname sysname = NULL,
                @owner_id int = NULL
            )
            WITH EXECUTE AS N''dbo''
            AS
            BEGIN
                DECLARE @user sysname
                DECLARE @dboLogin bit
                EXECUTE AS CALLER;
                    SET @user = USER_NAME();
                    SET @dboLogin = CONVERT(bit,IS_MEMBER(''db_owner''));
                REVERT;
                SELECT
                    [Database] = DB_NAME(),
                    [Name] = name,
                    [ID] = diagram_id,
                    [Owner] = USER_NAME(principal_id),
                    [OwnerID] = principal_id
                FROM
                    sysdiagrams
                WHERE
                    (@dboLogin = 1 OR USER_NAME(principal_id) = @user) AND
                    (@diagramname IS NULL OR name = @diagramname) AND
                    (@owner_id IS NULL OR principal_id = @owner_id)
                ORDER BY
                    4, 5, 1
            END
            '

            GRANT EXECUTE ON dbo.sp_helpdiagrams TO public
            DENY EXECUTE ON dbo.sp_helpdiagrams TO guest
        END

        IF OBJECT_ID(N'dbo.sp_helpdiagramdefinition') IS NULL and IS_MEMBER('db_owner') = 1
        BEGIN
            EXEC sp_executesql N'
            CREATE PROCEDURE dbo.sp_helpdiagramdefinition
            (
                @diagramname    sysname,
                @owner_id   int = null      
            )
            WITH EXECUTE AS N''dbo''
            AS
            BEGIN
                set nocount on

                declare @theId      int
                declare @IsDbo      int
                declare @DiagId     int
                declare @UIDFound   int
            
                if(@diagramname is null)
                begin
                    RAISERROR (N''E_INVALIDARG'', 16, 1);
                    return -1
                end
            
                execute as caller;
                select @theId = DATABASE_PRINCIPAL_ID();
                select @IsDbo = IS_MEMBER(N''db_owner'');
                if(@owner_id is null)
                    select @owner_id = @theId;
                revert; 
            
                select @DiagId = diagram_id, @UIDFound = principal_id from dbo.sysdiagrams where principal_id = @owner_id and name = @diagramname;
                if(@DiagId IS NULL or (@IsDbo = 0 and @UIDFound <> @theId ))
                begin
                    RAISERROR (''Diagram does not exist or you do not have permission.'', 16, 1);
                    return -3
                end

                select version, definition FROM dbo.sysdiagrams where diagram_id = @DiagId ; 
                return 0
            END
            '
            GRANT EXECUTE ON dbo.sp_helpdiagramdefinition TO public
            DENY EXECUTE ON dbo.sp_helpdiagramdefinition TO guest
        END

        IF OBJECT_ID(N'dbo.sp_creatediagram') IS NULL and IS_MEMBER('db_owner') = 1
        BEGIN
            EXEC sp_executesql N'
            CREATE PROCEDURE dbo.sp_creatediagram
            (
                @diagramname    sysname,
                @owner_id       int = null,
                @version        int,
                @definition     varbinary(max)
            )
            WITH EXECUTE AS ''dbo''
            AS
            BEGIN
                set nocount on
            
                declare @theId int
                declare @retval int
                declare @IsDbo  int
                declare @userName sysname
                if(@version is null or @diagramname is null)
                begin
                    RAISERROR (N''E_INVALIDARG'', 16, 1);
                    return -1
                end
            
                execute as caller;
                select @theId = DATABASE_PRINCIPAL_ID(); 
                select @IsDbo = IS_MEMBER(N''db_owner'');
                revert; 
                
                if @owner_id is null
                begin
                    select @owner_id = @theId;
                end
                else
                begin
                    if @theId <> @owner_id
                    begin
                        if @IsDbo = 0
                        begin
                            RAISERROR (N''E_INVALIDARG'', 16, 1);
                            return -1
                        end
                        select @theId = @owner_id
                    end
                end
                -- next 2 line only for test, will be removed after define name unique
                if EXISTS(select diagram_id from dbo.sysdiagrams where principal_id = @theId and name = @diagramname)
                begin
                    RAISERROR (''The name is already used.'', 16, 1);
                    return -2
                end
            
                insert into dbo.sysdiagrams(name, principal_id , version, definition)
                        VALUES(@diagramname, @theId, @version, @definition) ;
                
                select @retval = @@IDENTITY 
                return @retval
            END
            '
            GRANT EXECUTE ON dbo.sp_creatediagram TO public
            DENY EXECUTE ON dbo.sp_creatediagram TO guest
        END

        IF OBJECT_ID(N'dbo.sp_renamediagram') IS NULL and IS_MEMBER('db_owner') = 1
        BEGIN
            EXEC sp_executesql N'
            CREATE PROCEDURE dbo.sp_renamediagram
            (
                @diagramname        sysname,
                @owner_id       int = null,
                @new_diagramname    sysname
            
            )
            WITH EXECUTE AS ''dbo''
            AS
            BEGIN
                set nocount on
                declare @theId          int
                declare @IsDbo          int
                
                declare @UIDFound       int
                declare @DiagId         int
                declare @DiagIdTarg     int
                declare @u_name         sysname
                if((@diagramname is null) or (@new_diagramname is null))
                begin
                    RAISERROR (''Invalid value'', 16, 1);
                    return -1
                end
            
                EXECUTE AS CALLER;
                select @theId = DATABASE_PRINCIPAL_ID();
                select @IsDbo = IS_MEMBER(N''db_owner''); 
                if(@owner_id is null)
                    select @owner_id = @theId;
                REVERT;
            
                select @u_name = USER_NAME(@owner_id)
            
                select @DiagId = diagram_id, @UIDFound = principal_id from dbo.sysdiagrams where principal_id = @owner_id and name = @diagramname 
                if(@DiagId IS NULL or (@IsDbo = 0 and @UIDFound <> @theId))
                begin
                    RAISERROR (''Diagram does not exist or you do not have permission.'', 16, 1)
                    return -3
                end
            
                -- if((@u_name is not null) and (@new_diagramname = @diagramname))  -- nothing will change
                --  return 0;
            
                if(@u_name is null)
                    select @DiagIdTarg = diagram_id from dbo.sysdiagrams where principal_id = @theId and name = @new_diagramname
                else
                    select @DiagIdTarg = diagram_id from dbo.sysdiagrams where principal_id = @owner_id and name = @new_diagramname
            
                if((@DiagIdTarg is not null) and  @DiagId <> @DiagIdTarg)
                begin
                    RAISERROR (''The name is already used.'', 16, 1);
                    return -2
                end     
            
                if(@u_name is null)
                    update dbo.sysdiagrams set [name] = @new_diagramname, principal_id = @theId where diagram_id = @DiagId
                else
                    update dbo.sysdiagrams set [name] = @new_diagramname where diagram_id = @DiagId
                return 0
            END
            '
            GRANT EXECUTE ON dbo.sp_renamediagram TO public
            DENY EXECUTE ON dbo.sp_renamediagram TO guest
        END

        IF OBJECT_ID(N'dbo.sp_alterdiagram') IS NULL and IS_MEMBER('db_owner') = 1
        BEGIN
            EXEC sp_executesql N'
            CREATE PROCEDURE dbo.sp_alterdiagram
            (
                @diagramname    sysname,
                @owner_id   int = null,
                @version    int,
                @definition     varbinary(max)
            )
            WITH EXECUTE AS ''dbo''
            AS
            BEGIN
                set nocount on
            
                declare @theId          int
                declare @retval         int
                declare @IsDbo          int
                declare @UIDFound       int
                declare @DiagId         int
                declare @ShouldChangeUID    int
            
                if(@diagramname is null)
                begin
                    RAISERROR (''Invalid ARG'', 16, 1)
                    return -1
                end
            
                execute as caller;
                select @theId = DATABASE_PRINCIPAL_ID();
                select @IsDbo = IS_MEMBER(N''db_owner'');
                if(@owner_id is null)
                    select @owner_id = @theId;
                revert;
            
                select @ShouldChangeUID = 0
                select @DiagId = diagram_id, @UIDFound = principal_id from dbo.sysdiagrams where principal_id = @owner_id and name = @diagramname 
                
                if(@DiagId IS NULL or (@IsDbo = 0 and @theId <> @UIDFound))
                begin
                    RAISERROR (''Diagram does not exist or you do not have permission.'', 16, 1);
                    return -3
                end
            
                if(@IsDbo <> 0)
                begin
                    if(@UIDFound is null or USER_NAME(@UIDFound) is null) -- invalid principal_id
                    begin
                        select @ShouldChangeUID = 1 ;
                    end
                end

                -- update dds data          
                update dbo.sysdiagrams set definition = @definition where diagram_id = @DiagId ;

                -- change owner
                if(@ShouldChangeUID = 1)
                    update dbo.sysdiagrams set principal_id = @theId where diagram_id = @DiagId ;

                -- update dds version
                if(@version is not null)
                    update dbo.sysdiagrams set version = @version where diagram_id = @DiagId ;

                return 0
            END
            '

            GRANT EXECUTE ON dbo.sp_alterdiagram TO public
            DENY EXECUTE ON dbo.sp_alterdiagram TO guest
        END

        IF OBJECT_ID(N'dbo.sp_dropdiagram') IS NULL and IS_MEMBER('db_owner') = 1
        BEGIN
            EXEC sp_executesql N'
            CREATE PROCEDURE dbo.sp_dropdiagram
            (
                @diagramname    sysname,
                @owner_id   int = null
            )
            WITH EXECUTE AS ''dbo''
            AS
            BEGIN
                set nocount on
                declare @theId          int
                declare @IsDbo          int
                
                declare @UIDFound       int
                declare @DiagId         int
            
                if(@diagramname is null)
                begin
                    RAISERROR (''Invalid value'', 16, 1);
                    return -1
                end
            
                EXECUTE AS CALLER;
                select @theId = DATABASE_PRINCIPAL_ID();
                select @IsDbo = IS_MEMBER(N''db_owner''); 
                if(@owner_id is null)
                    select @owner_id = @theId;
                REVERT; 
                
                select @DiagId = diagram_id, @UIDFound = principal_id from dbo.sysdiagrams where principal_id = @owner_id and name = @diagramname 
                if(@DiagId IS NULL or (@IsDbo = 0 and @UIDFound <> @theId))
                begin
                    RAISERROR (''Diagram does not exist or you do not have permission.'', 16, 1)
                    return -3
                end
            
                delete from dbo.sysdiagrams where diagram_id = @DiagId;
            
                return 0;
            END
            '
            GRANT EXECUTE ON dbo.sp_dropdiagram TO public
            DENY EXECUTE ON dbo.sp_dropdiagram TO guest
        END

        IF OBJECT_ID(N'dbo.fn_diagramobjects') IS NULL and IS_MEMBER('db_owner') = 1
        BEGIN
            EXEC sp_executesql N'
            CREATE FUNCTION dbo.fn_diagramobjects() 
            RETURNS int
            WITH EXECUTE AS N''dbo''
            AS
            BEGIN
                declare @id_upgraddiagrams      int
                declare @id_sysdiagrams         int
                declare @id_helpdiagrams        int
                declare @id_helpdiagramdefinition   int
                declare @id_creatediagram   int
                declare @id_renamediagram   int
                declare @id_alterdiagram    int 
                declare @id_dropdiagram     int
                declare @InstalledObjects   int

                select @InstalledObjects = 0

                select  @id_upgraddiagrams = object_id(N''dbo.sp_upgraddiagrams''),
                    @id_sysdiagrams = object_id(N''dbo.sysdiagrams''),
                    @id_helpdiagrams = object_id(N''dbo.sp_helpdiagrams''),
                    @id_helpdiagramdefinition = object_id(N''dbo.sp_helpdiagramdefinition''),
                    @id_creatediagram = object_id(N''dbo.sp_creatediagram''),
                    @id_renamediagram = object_id(N''dbo.sp_renamediagram''),
                    @id_alterdiagram = object_id(N''dbo.sp_alterdiagram''), 
                    @id_dropdiagram = object_id(N''dbo.sp_dropdiagram'')


                if @id_upgraddiagrams is not null
                    select @InstalledObjects = @InstalledObjects + 1
                if @id_sysdiagrams is not null
                    select @InstalledObjects = @InstalledObjects + 2
                if @id_helpdiagrams is not null
                    select @InstalledObjects = @InstalledObjects + 4
                if @id_helpdiagramdefinition is not null
                    select @InstalledObjects = @InstalledObjects + 8
                if @id_creatediagram is not null
                    select @InstalledObjects = @InstalledObjects + 16
                if @id_renamediagram is not null
                    select @InstalledObjects = @InstalledObjects + 32
                if @id_alterdiagram  is not null
                    select @InstalledObjects = @InstalledObjects + 64
                if @id_dropdiagram is not null
                    select @InstalledObjects = @InstalledObjects + 128
                
                return @InstalledObjects 
            END
            '

            GRANT EXECUTE ON dbo.fn_diagramobjects TO public
            DENY EXECUTE ON dbo.fn_diagramobjects TO guest
        END

        if IS_MEMBER('db_owner') = 1
        BEGIN
            declare @val int
            select @val = 1
            if NOT EXISTS(  select major_id 
                            from sys.extended_properties
                            where major_id = object_id(N'dbo.sysdiagrams') and class = 1 and minor_id = 0 and name = N'microsoft_database_tools_support')
            begin
                exec sp_addextendedproperty N'microsoft_database_tools_support', @val, 'SCHEMA', N'dbo', 'TABLE', N'sysdiagrams', NULL, NULL
            end
            
            if NOT EXISTS(  select major_id 
                            from sys.extended_properties
                            where major_id = object_id(N'dbo.sp_upgraddiagrams') and class = 1 and minor_id = 0 and name = N'microsoft_database_tools_support')
            begin
                exec sp_addextendedproperty N'microsoft_database_tools_support', @val, 'SCHEMA', N'dbo', 'PROCEDURE', N'sp_upgraddiagrams', NULL, NULL
            end
            
            if NOT EXISTS(  select major_id 
                            from sys.extended_properties
                            where major_id = object_id(N'dbo.sp_helpdiagrams') and class = 1 and minor_id = 0 and name = N'microsoft_database_tools_support')
            begin
                exec sp_addextendedproperty N'microsoft_database_tools_support', @val, 'SCHEMA', N'dbo', 'PROCEDURE', N'sp_helpdiagrams', NULL, NULL
            end
            
            if NOT EXISTS(  select major_id 
                            from sys.extended_properties
                            where major_id = object_id(N'dbo.sp_helpdiagramdefinition') and class = 1 and minor_id = 0 and name = N'microsoft_database_tools_support')
            begin
                exec sp_addextendedproperty N'microsoft_database_tools_support', @val, 'SCHEMA', N'dbo', 'PROCEDURE', N'sp_helpdiagramdefinition', NULL, NULL
            end
            
            if NOT EXISTS(  select major_id 
                            from sys.extended_properties
                            where major_id = object_id(N'dbo.sp_creatediagram') and class = 1 and minor_id = 0 and name = N'microsoft_database_tools_support')
            begin
                exec sp_addextendedproperty N'microsoft_database_tools_support', @val, 'SCHEMA', N'dbo', 'PROCEDURE', N'sp_creatediagram', NULL, NULL
            end
            
            if NOT EXISTS(  select major_id 
                            from sys.extended_properties
                            where major_id = object_id(N'dbo.sp_renamediagram') and class = 1 and minor_id = 0 and name = N'microsoft_database_tools_support')
            begin
                exec sp_addextendedproperty N'microsoft_database_tools_support', @val, 'SCHEMA', N'dbo', 'PROCEDURE', N'sp_renamediagram', NULL, NULL
            end
            
            if NOT EXISTS(  select major_id 
                            from sys.extended_properties
                            where major_id = object_id(N'dbo.sp_alterdiagram') and class = 1 and minor_id = 0 and name = N'microsoft_database_tools_support')
            begin
                exec sp_addextendedproperty N'microsoft_database_tools_support', @val, 'SCHEMA', N'dbo', 'PROCEDURE', N'sp_alterdiagram', NULL, NULL
            end
            
            if NOT EXISTS(  select major_id 
                            from sys.extended_properties
                            where major_id = object_id(N'dbo.sp_dropdiagram') and class = 1 and minor_id = 0 and name = N'microsoft_database_tools_support')
            begin
                exec sp_addextendedproperty N'microsoft_database_tools_support', @val, 'SCHEMA', N'dbo', 'PROCEDURE', N'sp_dropdiagram', NULL, NULL
            end
            
            if NOT EXISTS(  select major_id 
                            from sys.extended_properties
                            where major_id = object_id(N'dbo.fn_diagramobjects') and class = 1 and minor_id = 0 and name = N'microsoft_database_tools_support')
            begin
                exec sp_addextendedproperty N'microsoft_database_tools_support', @val, 'SCHEMA', N'dbo', 'FUNCTION', N'fn_diagramobjects', NULL, NULL
            end
        END`;
    },
    disableDatabaseDiagrams: function(name) {
        return `
        USE [${name}]

        IF OBJECT_ID(N'dbo.fn_diagramobjects') IS NULL and IS_MEMBER('db_owner') = 1
            DROP FUNCTION dbo.fn_diagramobjects

        IF OBJECT_ID(N'dbo.sp_dropdiagram') IS NULL and IS_MEMBER('db_owner') = 1
            DROP PROCEDURE dbo.sp_dropdiagram
        
        IF OBJECT_ID(N'dbo.sp_alterdiagram') IS NULL and IS_MEMBER('db_owner') = 1
            DROP PROCEDURE dbo.sp_alterdiagram

        IF OBJECT_ID(N'dbo.sp_renamediagram') IS NULL and IS_MEMBER('db_owner') = 1
            DROP PROCEDURE dbo.sp_renamediagram
        
        IF OBJECT_ID(N'dbo.sp_creatediagram') IS NULL and IS_MEMBER('db_owner') = 1
            DROP PROCEDURE dbo.sp_creatediagram

        IF OBJECT_ID(N'dbo.sp_helpdiagramdefinition') IS NULL and IS_MEMBER('db_owner') = 1
            DROP PROCEDURE dbo.sp_helpdiagramdefinition
        
        IF OBJECT_ID(N'dbo.sp_helpdiagrams') IS NULL and IS_MEMBER('db_owner') = 1
            DROP PROCEDURE dbo.sp_helpdiagrams

        IF OBJECT_ID(N''dbo.sysdiagrams'') IS NOT NULL and IS_MEMBER('db_owner') = 1
            DROP TABLE dbo.sysdiagrams
        
        IF OBJECT_ID(N'dbo.sp_upgraddiagrams') IS NULL and IS_MEMBER('db_owner') = 1
            DROP PROCEDURE dbo.sp_upgraddiagrams`;
    }
};
