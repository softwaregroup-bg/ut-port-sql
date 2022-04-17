const path = require('path');
/* eslint-disable no-template-curly-in-string */
require('ut-run').run({
    main: [
        () => ({
            test: () => [
                (...params) => class db extends require('../')(...params) {},
                function sql() {
                    return {
                        namespace: 'test',
                        schema: [{
                            path: path.join(__dirname, 'oracle'),
                            linkSP: true
                        }],
                        seed: [{
                            path: path.join(__dirname, 'oracleSeed')
                        }],
                        'test.test.deadlock': function(_, $meta) {
                            return Promise.all([
                                this.exec({}, {method: 'test.test.selectHoldLock'}),
                                this.exec({reverse: true}, {method: 'test.test.selectHoldLock'})
                            ]);
                        }
                    };
                }
            ]
        })
    ],
    method: 'unit',
    config: {
        implementation: 'port-sql',
        test: true,
        db: {
            imports: ['sql'],
            allowQuery: true,
            logLevel: 'warn',
            linkSP: true,
            connection: {
                driver: 'oracle',
                domain: 'softwaregroup-bg.com',
                server: 'bgs-vlx-db-05.softwaregroup-bg.com:1521',
                user: '${decrypt(\'3b280fb6a2c0c22483dfb73be18128774fa156653edd29eebed4f3c4e8f5c0fa\')}',
                password: '${decrypt(\'de763840f0dc08b85b0d845b17d15e1bdaf6a774dc4eecf32e368620b7b7d410\')}'
            },
            create: {
                user: '${decrypt(\'4a3a31e5593ec8f259bf116e0e22a34658c157209148be9abc1ef9356bc75016\')}'
            }
        }
    },
    params: {
        steps: [
            {
                name: 'exec',
                method: 'test.query',
                params: {
                    query: 'SELECT 1 AS "test" FROM DUAL',
                    process: 'json'
                },
                result: (result, assert) => {
                    assert.ok(Array.isArray(result.dataSet), 'result returned');
                    assert.equal(result.dataSet[0].test, 1, 'result correctness checked');
                }
            },
            {
                name: 'result set',
                method: 'test.resultset',
                params: {
                    message: 'hello world'
                },
                result: (result, assert) => {
                    assert.match(result, {result: [{column: 'hello world'}]}, 'result set');
                }
            }
        ]
    }
});
