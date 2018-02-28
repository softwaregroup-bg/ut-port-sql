module.exports = (statement) => {
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
};
