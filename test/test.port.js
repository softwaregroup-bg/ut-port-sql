require('ut-run').run({
    main: require('..'),
    method: 'unit',
    config: {
        SqlPort: {
            allowQuery: true,
            connection: {
                server: 'BGS-VWS-DB-02',
                database: 'ut-port-sql-test',
                user: 'utPortSql',
                password: 'test123'
            },
            create: {
                user: 'ut5',
                password: 'ut5'
            }
        }
    },
    params: {
        steps: [
            {
                method: 'SqlPort.query',
                name: 'exec',
                params: {
                    query: 'SELECT 1 AS test',
                    process: 'json'
                },
                result: (result, assert) => {
                    assert.true(Array.isArray(result.dataSet));
                    assert.equals(result.dataSet[0].test, 1);
                }
            }
        ]
    }
});
