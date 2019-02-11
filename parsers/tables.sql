SELECT
    so.create_date,
    so.object_id,
    NULL,
    RTRIM(so.[type]) [type],
    SCHEMA_NAME(so.schema_id) [namespace],
    so.Name AS [name],
    SCHEMA_NAME(so.schema_id) + '.' + so.Name AS [full],
    'CREATE TABLE [' + SCHEMA_NAME(so.schema_id) + '].[' + so.name + '](' + CHAR(13) + CHAR(10) + o.list +
    CASE
        WHEN tc.Constraint_Name IS NULL THEN ''
        ELSE '    CONSTRAINT [' + tc.Constraint_Name + '] PRIMARY KEY ' + '(' + LEFT(j.List, LEN(j.List) - 1) + ')' + CHAR(13) + CHAR(10) + ')'
    END [source]
FROM
    sys.objects so
CROSS APPLY (
    SELECT
        '    [' + column_name + '] ' +
        data_type + CASE data_type
            WHEN 'sql_variant' THEN ''
            WHEN 'text' THEN ''
            WHEN 'ntext' THEN ''
            WHEN 'xml' THEN ''
            WHEN 'decimal' THEN '(' + CAST(numeric_precision AS varchar) + ', ' + CAST(numeric_scale AS varchar) + ')'
            ELSE COALESCE('(' + CASE WHEN character_maximum_length = -1 THEN 'MAX' ELSE CAST(character_maximum_length AS varchar) END + ')', '') END +
        CASE WHEN EXISTS (
        SELECT id FROM syscolumns
        WHERE id = so.object_id
        AND name = column_name
        AND COLUMNPROPERTY(id, name, 'IsIdentity') = 1
        ) THEN
        ' IDENTITY(' +
        CAST(ident_seed(SCHEMA_NAME(so.schema_id) + '.' + so.name) AS varchar) + ',' +
        CAST(ident_incr(SCHEMA_NAME(so.schema_id) + '.' + so.name) AS varchar) + ') '
        ELSE ' '
        END +
        CASE WHEN IS_NULLABLE = 'No' THEN 'NOT ' ELSE '' END + 'NULL' +
        CASE WHEN information_schema.columns.COLUMN_DEFAULT IS NOT NULL THEN ' DEFAULT ' + information_schema.columns.COLUMN_DEFAULT ELSE '' END + ',' + CHAR(10)
    FROM
        information_schema.columns WHERE table_name = so.name
    ORDER BY
        ordinal_position
    FOR XML PATH('')) o (list)
LEFT JOIN
    information_schema.table_constraints tc ON tc.Table_name = so.Name AND tc.Constraint_Type = 'PRIMARY KEY'
CROSS APPLY (
    SELECT '[' + Column_Name + '], '
    FROM
        information_schema.key_column_usage kcu
    WHERE
        kcu.Constraint_Name = tc.Constraint_Name
    ORDER BY
        ORDINAL_POSITION
    FOR XML PATH('')) j (list)
WHERE
    TYPE = 'U' AND
    USER_NAME(OBJECTPROPERTY(so.object_id, 'OwnerId')) IN (USER_NAME(), 'dbo') AND
    OBJECTPROPERTY(so.object_id, 'IsMSShipped') = 0
