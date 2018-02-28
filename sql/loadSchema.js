module.exports = (partial) => {
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
            ELSE ${partial ? `LEFT(c.text, CASE CHARINDEX(CHAR(10)+'AS'+CHAR(13), c.text) WHEN 0 THEN 2500 ELSE CHARINDEX(CHAR(10)+'AS'+CHAR(13), c.text) + 10 END)` : 'c.text'}
        END AS [source]

    FROM
        sys.objects o
    LEFT JOIN
        dbo.syscomments c on o.object_id = c.id
    LEFT JOIN
        sys.synonyms s on s.object_id = o.object_id
    WHERE
        o.type IN (${partial ? `'P'` : `'V', 'P', 'FN','F','IF','SN','TF','TR','U'`}) AND
        user_name(objectproperty(o.object_id, 'OwnerId')) in (USER_NAME(),'dbo') AND
        objectproperty(o.object_id, 'IsMSShipped') = 0 AND
        SCHEMA_NAME(o.schema_id) != 'dbo'
        ${partial ? 'AND ISNULL(c.colid, 1)=1' : ''}
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
};
