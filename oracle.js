'use strict';

module.exports = {
    getHash() {
        return `
        DECLARE C1 SYS_REFCURSOR;
        BEGIN
            FOR i IN (SELECT null FROM user_views WHERE view_name = 'utSchemaHash') LOOP
                OPEN C1 FOR SELECT hash FROM "utSchemaHash";
                DBMS_SQL.RETURN_RESULT(C1);
            END LOOP;
        END;
        `;
    },
    setHash(hash) {
        return `CREATE VIEW "utSchemaHash" AS SELECT '${hash}' hash FROM DUAL`;
    },
    dropHash() {
        return `
        BEGIN
            FOR i IN (SELECT null FROM user_views WHERE view_name = 'utSchemaHash') LOOP
                EXECUTE IMMEDIATE 'DROP VIEW "utSchemaHash"';
            END LOOP;
        END;
        `;
    },
    loadSchema(partial, loadDbo) {
        return `
            SELECT
                1 "colid",
                CASE o.object_type
                    WHEN 'PROCEDURE' THEN 'P'
                END "type",
                SUBSTR(o.object_name, 1, INSTR(o.object_name, '.') - 1) "namespace",
                SUBSTR(o.object_name, INSTR(o.object_name, '.') + 1) "name",
                o.object_name "full",
                t.source "source"
            FROM
                user_objects o
            JOIN (
                SELECT
                    type,
                    name,
                    'CREATE OR REPLACE ' || LISTAGG(text, Chr(10)) WITHIN GROUP (ORDER BY line) AS source
                FROM
                    user_source
                WHERE
                    rownum <= 200
                GROUP BY
                    type, name
            ) t ON o.object_name = t.name AND o.object_type = t.type
        `;
    },
    refreshView(drop) {
        return 'SELECT 1 AS test FROM dual WHERE 1=2';
    },
    auditLog(statement) {
        if (!statement.params) {
            return;
        }
        let sql = '    DECLARE @callParams XML = ( SELECT ';
        statement.params.map(function(param) {
            if (param.def.type === 'table') {
                sql += `(SELECT * FROM @${param.name} rows FOR XML AUTO, BINARY BASE64, TYPE) [${param.name}], `;
            } else {
                sql += `@${param.name} [${param.name}], `;
            }
        });
        sql = sql.replace(/,\s$/, ' ');
        sql += 'FOR XML RAW(\'params\'), BINARY BASE64, TYPE) EXEC core.auditCall @procid = @@PROCID, @params=@callParams';
        return sql;
    },
    callParams: statement => '',
    databaseExists(name) {
        name = name.replace(/-/g, '_').toUpperCase();
        return `SELECT COUNT(*) "exists" FROM DBA_PDBS WHERE PDB_NAME='${name}'`;
    },
    createDatabase(name, level, user, password) {
        name = name.replace(/-/g, '_');
        return `
            CREATE PLUGGABLE DATABASE ${name}
            ADMIN USER "${user.toUpperCase()}"
            IDENTIFIED BY "${password}"
            ROLES=(DBA)
            FILE_NAME_CONVERT = ('/pdbseed/', '/${name}/')
        `;
    },
    openDatabase(name) {
        name = name.replace(/-/g, '_');
        return `
            ALTER PLUGGABLE DATABASE ${name} OPEN READ WRITE
        `;
    },
    createUser(name, user, password, {azure = false}) {
        return `
            SELECT * FROM all_users WHERE username = '${name}'
        `;
    },
    ngramIndex: (schema, table) => '',
    ngramIndexById: (schema, table) => '',
    ngramIndexTT: (schema) => '',
    ngramTT: (schema) => '',
    ngramMerge: (schema, table) => '',
    ngramSearch: (schema, table) => '',
    enableDatabaseDiagrams(name) {
        return '';
    },
    disableDatabaseDiagrams(name) {
        return '';
    },
    permissionCheck: ({
        '@actionId': actionId = 'OBJECT_SCHEMA_NAME(@@PROCID) + \'.\' + OBJECT_NAME(@@PROCID)',
        '@objectId': objectId = 'NULL',
        offset = '',
        ...rest
    }) => ''
};
