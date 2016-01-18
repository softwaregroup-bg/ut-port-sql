select  
    so.create_date,
    so.object_id,
    null,
    RTRIM(so.[type]) [type],
    SCHEMA_NAME(so.schema_id) [namespace],
    so.Name AS [name],
    SCHEMA_NAME(so.schema_id) + '.' + so.Name AS [full],
    'CREATE TABLE [' + SCHEMA_NAME(so.schema_id) + '].[' + so.name + ']('+CHAR(13)+CHAR(10) + o.list 
    + CASE WHEN tc.Constraint_Name IS NULL THEN '' ELSE '    CONSTRAINT [' + tc.Constraint_Name  + '] PRIMARY KEY ' + '(' + LEFT(j.List, Len(j.List)-1) + ')' 
    + CHAR(13) + CHAR(10) + ')' END [source]
from
    sys.objects so
cross apply
    (SELECT 
        '    ['+column_name+'] ' + 
        data_type + case data_type
            when 'sql_variant' then ''
            when 'text' then ''
            when 'ntext' then ''
            when 'xml' then ''
            when 'decimal' then '(' + cast(numeric_precision as varchar) + ', ' + cast(numeric_scale as varchar) + ')'
            else coalesce('('+case when character_maximum_length = -1 then 'MAX' else cast(character_maximum_length as varchar) end +')','') end +
        case when exists ( 
        select id from syscolumns
        where id=so.object_id
        and name=column_name
        and columnproperty(id,name,'IsIdentity') = 1 
        ) then
        ' IDENTITY(' + 
        cast(ident_seed(SCHEMA_NAME(so.schema_id) + '.' + so.name) as varchar) + ',' + 
        cast(ident_incr(SCHEMA_NAME(so.schema_id) + '.' + so.name) as varchar) + ') '
        else ' '
        end +
        case when IS_NULLABLE = 'No' then 'NOT ' else '' end + 'NULL' + 
        case when information_schema.columns.COLUMN_DEFAULT IS NOT NULL THEN ' DEFAULT '+ information_schema.columns.COLUMN_DEFAULT ELSE '' END + ',' + CHAR(10)
     from 
        information_schema.columns where table_name = so.name
     order by 
        ordinal_position
    FOR XML PATH('')) o (list)
left join
    information_schema.table_constraints tc on tc.Table_name = so.Name AND tc.Constraint_Type = 'PRIMARY KEY'
cross apply
    (select '[' + Column_Name + '], '
     FROM
        information_schema.key_column_usage kcu
     WHERE
        kcu.Constraint_Name = tc.Constraint_Name
     ORDER BY
        ORDINAL_POSITION
     FOR XML PATH('')) j (list)
where
    type = 'U' AND
    user_name(objectproperty(so.object_id, 'OwnerId')) in (USER_NAME(),'dbo') AND
    objectproperty(so.object_id, 'IsMSShipped') = 0
