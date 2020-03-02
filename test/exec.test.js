/* eslint-disable no-template-curly-in-string */
require('ut-run').run({
    main: (...params) => class db extends (require('../')(...params)) {},
    method: 'unit',
    config: {
        implementation: 'port-sql',
        db: {
            allowQuery: true,
            logLevel: 'error'
        }
    },
    params: {
        steps: [
            {
                method: 'db.query',
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
