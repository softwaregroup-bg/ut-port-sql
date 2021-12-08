const path = require('path');
/* eslint-disable no-template-curly-in-string */
require('ut-run').run({
    main: [
        () => ({
            test: () => [
                (...params) => class db extends require('../')(...params) {},
                function test() {
                    return {
                        schema: [{
                            path: path.join(__dirname, 'schema'),
                            linkSP: true
                        }],
                        seed: [{
                            path: path.join(__dirname, 'seed')
                        }],
                        'test.test.deadlock': function(_, $meta) {
                            return Promise.all([
                                this.bus.importMethod('db/test.test.selectHoldLock')({}, $meta),
                                this.bus.importMethod('db/test.test.selectHoldLock')({reversed: true}, $meta)
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
            namespace: 'db/test',
            imports: ['test'],
            allowQuery: true,
            logLevel: 'error',
            connection: {
                server: 'infradb14',
                user: '${decrypt(\'3b280fb6a2c0c22483dfb73be18128774fa156653edd29eebed4f3c4e8f5c0fa\')}',
                password: '${decrypt(\'de763840f0dc08b85b0d845b17d15e1bdaf6a774dc4eecf32e368620b7b7d410\')}'
            },
            create: {
                user: '${decrypt(\'289fd8ff4717c56d59b1ebc6987fbd1f1f0df4849705f6216b319763c8edb252\')}'
            }
        }
    },
    params: {
        steps: [
            {
                name: 'exec',
                method: 'db/test.query',
                params: {
                    query: 'SELECT 1 AS test',
                    process: 'json'
                },
                result: (result, assert) => {
                    assert.ok(Array.isArray(result.dataSet), 'result returned');
                    assert.equal(result.dataSet[0].test, 1, 'result correctness checked');
                }
            },
            {
                name: 'exec',
                method: 'test.test.deadlock',
                params: {},
                result: (result, assert) => {
                    assert.ok(result, 'result returned');
                },
                error: (error, assert) => {
                    assert.ok(error, 'error returned');
                }
            }
        ]
    }
});
