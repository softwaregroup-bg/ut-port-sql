module.exports = class OracleRequest {
    /**
     * @param {Promise<import('oracledb').Pool>} pool
     */
    constructor(pool) {
        this.pool = pool;
        const oracle = this.oracle = require('oracledb');
        this.directions = {
            in: oracle.BIND_IN,
            out: oracle.BIND_OUT,
            inout: oracle.BIND_INOUT
        };
        this.type = {
            varchar2: oracle.DB_TYPE_VARCHAR,
            char: oracle.DB_TYPE_CHAR,
            nvarchar: oracle.DB_TYPE_NVARCHAR,
            nchar: oracle.DB_TYPE_NCHAR,
            number: oracle.DB_TYPE_NUMBER,
            binary_double: oracle.DB_TYPE_BINARY_DOUBLE,
            binary_float: oracle.DB_TYPE_BINARY_FLOAT,
            binary_integer: oracle.DB_TYPE_BINARY_INTEGER,
            date: oracle.DB_TYPE_DATE,
            timestamp: oracle.DB_TYPE_TIMESTAMP,
            raw: oracle.DB_TYPE_RAW,
            clob: oracle.DB_TYPE_CLOB,
            blob: oracle.DB_TYPE_BLOB,
            nclob: oracle.DB_TYPE_NCLOB,
            rowid: oracle.DB_TYPE_VARCHAR,
            urowid: oracle.DB_TYPE_VARCHAR,
            json: oracle.DB_TYPE_JSON,
            xmltype: oracle.DB_TYPE_VARCHAR,
            cursor: oracle.DB_TYPE_CURSOR
        };
        this.parameters = [];
    }

    input(name, type, value) {
        this.parameters.push([name, value, type, 'input']);
    }

    output(name, type, value) {
        this.parameters.push([name, value, type, 'output']);
    }

    /**
     * @param {import('oracledb').ResultSet<{}>[]} resultSets
     */
    async results(resultSets = [], outBinds, outNames) {
        const results = [];
        for (const resultSet of resultSets) {
            results.push(await resultSet.getRows());
        };
        for (const index in outNames) {
            results.push([{resultSetName: outNames[index]}]);
            const value = outBinds[index];
            results.push(typeof value.getRows === 'function' ? await outBinds[index].getRows() : value);
        };
        return results;
    }

    async query(query, options) {
        const connection = await this.pool.getConnection();
        try {
            const params = this.parameters.map(([, val, type]) => {
                const dir = this.directions[type?.dir || 'in'];
                switch (type?.type) {
                    case 'nested': return {
                        type: `"${type.typeName.replace('.', '"."')}"`,
                        val,
                        dir
                    };
                    case 'sys_refcursor': return {
                        type: this.oracle.CURSOR,
                        dir
                    };
                    default: return {
                        type: this.type[type.type] || this.oracle.DB_TYPE_VARCHAR,
                        val,
                        dir
                    };
                }
            });
            const outNames = params.map((param, index) => [this.oracle.BIND_INOUT, this.oracle.BIND_OUT].includes(param?.dir) && this.parameters[index][0]).filter(Boolean);
            const result = await connection.execute(
                query,
                params,
                options || {}
            );
            const recordsets = (result.implicitResults || outNames.length)
                ? await this.results(result.implicitResults, result.outBinds, outNames)
                : result.rows
                    ? [result.rows]
                    : [];
            return {
                recordsets,
                recordset: recordsets?.[0]
            };
        } finally {
            connection.close();
        }
    }

    execute(procedure) {
        return this.query(`BEGIN "${procedure}"(${this.parameters.map(([name]) => ':' + name).join(', ')}); COMMIT; END;`, {resultSet: true});
    }

    batch(query) {
        return this.query(query);
    }
};
