module.exports = class OracleRequest {
    /**
     * @param {Promise<import('oracledb').Pool>} pool
     */
    constructor(pool) {
        this.pool = pool;
        this.parameters = [];
    }

    input(name, type, value) {
        this.parameters.push([name, value, type, 'input']);
    }

    output(name, type, value) {
        this.parameters.push([name, value, type, 'output']);
    }

    async results(resultSets) {
        const results = [];
        for (const resultSet of resultSets) {
            const rows = [];
            let row;
            while ((row = await resultSet.getRow())) {
                rows.push(row);
            }
            results.push(rows);
        };
        return results;
    }

    async query(query, options) {
        const connection = await this.pool.getConnection();
        try {
            const result = await connection.execute(
                query,
                this.parameters.map(([, param]) => param),
                options || {}
            );
            const recordsets = result.implicitResults
                ? await this.results(result.implicitResults)
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
        return this.query(`BEGIN "${procedure}"(${this.parameters.map(([name]) => ':' + name).join(', ')}); END;`, {resultSet: true});
    }

    batch(query) {
        return this.query(query);
    }
};
