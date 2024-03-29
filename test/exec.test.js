/* eslint-disable no-template-curly-in-string */
const path = require('path');

const connect = {
    logLevel: 'warn',
    cbc: '75742d706f72742d73716c2121212d2d2d2d75742d706f72742d73716c212121',
    hmac: '75742d706f72742d73716c2121212d2d2d2d75742d706f72742d73716c212121',
    connection: {
        server: 'infradb14',
        // eslint-disable-next-line no-process-env
        database: process.env.ut_port_sql_jenkins_db__connection__database, // we need this for the dbAlter port
        user: '${decrypt(\'3b280fb6a2c0c22483dfb73be18128774fa156653edd29eebed4f3c4e8f5c0fa\')}',
        password: '${decrypt(\'de763840f0dc08b85b0d845b17d15e1bdaf6a774dc4eecf32e368620b7b7d410\')}'
    },
    create: {
        user: '${decrypt(\'289fd8ff4717c56d59b1ebc6987fbd1f1f0df4849705f6216b319763c8edb252\')}'
    }
};

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
                (...params) => class dbAlter extends require('../')(...params) {},
                function sqlAlter() {
                    return {
                        namespace: 'testAlter',
                        schema: [{
                            path: path.join(__dirname, 'schemaAlter'),
                            permissionCheck: true,
                            linkSP: true,
                            createTT: true
                        }]
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
            cover: true,
            ...connect
        },
        dbAlter: {
            imports: ['sqlAlter'],
            alterTable: true,
            ...connect
        },
        utRun: {
            test: {
                jobs: 'test'
            }
        }
    }
});
