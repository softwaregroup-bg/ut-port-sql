const path = require('path');
const id = Date.now();
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
                            createTT: true,
                            linkSP: true
                        }],
                        seed: [{
                            path: path.join(__dirname, 'oracleSeed')
                        }]
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
                user: '${decrypt(\'289fd8ff4717c56d59b1ebc6987fbd1f1f0df4849705f6216b319763c8edb252\')}'
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
                name: 'params direction',
                method: 'test.procedure',
                params: {
                    testInput: 'input',
                    testInputOutput: 'input-output'
                },
                result: (result, assert) => {
                    assert.strictSame(result, {
                        testInputOutput: 'input/input-output',
                        testOutputNumber: 555,
                        testOutputString: 'output-string'
                    }, 'out params');
                }
            },
            {
                name: 'result set',
                method: 'test.resultset',
                params: {
                    message: 'hello world'
                },
                result: (result, assert) => {
                    assert.strictSame(result, {result: [{column: 'hello world'}]}, 'result set');
                }
            },
            {
                name: 'table parameter',
                method: 'test.property.add',
                params: {
                    property: [{
                        id,
                        name: 'test name',
                        value: 'test value'
                    }]
                },
                result: (result, assert) => {
                    assert.strictSame(result, {
                        result: [{id, name: 'test name', value: 'test value'}],
                        resultCursor: [{id, name: 'test name', value: 'test value'}]
                    }, 'result set');
                }
            }
        ]
    }
});
