const path = require('path');
/* eslint-disable no-template-curly-in-string */
require('ut-run').run({
    main: [
        () => ({
            test: () => [
                require('./errors'),
                (...params) => class db extends require('../')(...params) {},
                function sql() {
                    return {
                        namespace: 'test',
                        schema: [{
                            path: path.join(__dirname, 'schema'),
                            permissionCheck: true,
                            linkSP: true,
                            createTT: true
                        }],
                        seed: [{
                            path: path.join(__dirname, 'seed')
                        }],
                        'test.test.deadlock': function(_, $meta) {
                            return Promise.all([
                                this.exec({}, {method: 'test.test.selectHoldLock'}),
                                this.exec({reverse: true}, {method: 'test.test.selectHoldLock'})
                            ]);
                        },
                        'test.test.error': async function(params, $meta) {
                            try {
                                await this.exec(params, {method: 'test.test.stack'});
                            } catch (error) {
                                error.message += '\n' + error.stack;
                                throw error;
                            }
                        }
                    };
                },
                ...require('./jobs')
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
            cover: true,
            cbc: '75742d706f72742d73716c2121212d2d2d2d75742d706f72742d73716c212121',
            hmac: '75742d706f72742d73716c2121212d2d2d2d75742d706f72742d73716c212121',
            connection: {
                server: 'bgs-vws-db-10.softwaregroup-bg.com',
                user: 'kalin.krustev',
                password: 'kalin@bgs-vws-db-10'
            },
            create: {
                user: 'ut5',
                password: 'ut5'
            }
        },
        utRun: {
            test: {
                jobs: 'test'
            },
            logLevel: 'trace'
        }
    }
});
