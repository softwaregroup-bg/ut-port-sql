ALTER PROCEDURE test.[test.table]
    @table NVARCHAR(128)
AS
SELECT 'table' AS resultSetName
SELECT
    c.name [column],
    st.name type,
    CASE
        WHEN st.name IN ('decimal', 'numeric') THEN CAST(c.[precision] AS VARCHAR)
        WHEN st.name IN ('datetime2', 'time', 'datetimeoffset') THEN CAST(c.[scale] AS VARCHAR)
        WHEN st.name IN ('varchar', 'varbinary', 'char', 'binary') AND c.max_length >= 0 THEN CAST(c.max_length AS VARCHAR)
        WHEN st.name IN ('nvarchar', 'nchar') AND c.max_length >= 0 THEN CAST(c.max_length / 2 AS VARCHAR)
        WHEN st.name IN ('varchar', 'varbinary', 'char', 'binary', 'nvarchar', 'nchar') AND c.max_length < 0 THEN 'max'
    END [length],
    CASE
        WHEN st.name IN ('decimal', 'numeric') THEN c.scale
    END scale,
    OBJECT_DEFINITION(c.default_object_id) [default]
FROM
    sys.objects o
JOIN
    sys.columns c ON o.object_id = c.object_id
JOIN
    sys.systypes AS st ON st.xusertype = c.user_type_id
WHERE
    SCHEMA_NAME(o.schema_id) + '.' + o.name = @table
ORDER BY
    c.name
