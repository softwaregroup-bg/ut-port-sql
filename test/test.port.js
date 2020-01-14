/* eslint-disable no-template-curly-in-string */
require('ut-run').run({
    main: require('..'),
    method: 'unit',
    config: {
        SqlPort: {
            allowQuery: true,
            connection: {
                server: 'BGS-VWS-DB-02',
                database: 'ut-port-sql-test',
                user: '${decrypt("3b280fb6a2c0c22483dfb73be18128774fa156653edd29eebed4f3c4e8f5c0fa")}',
                password: '${decrypt("de763840f0dc08b85b0d845b17d15e1bdaf6a774dc4eecf32e368620b7b7d410")}'
            },
            create: {
                user: '${decrypt("289fd8ff4717c56d59b1ebc6987fbd1f1f0df4849705f6216b319763c8edb252")}',
                password: '${decrypt("3569bf662a23fad6eb2069e09e8da490dff37a84e69a5eb82a1efecf9f8fcdb2")}'
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
