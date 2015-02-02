require('repl').start({useGlobal: true});

var wire = require('wire');

m = wire({
    bunyan: {
        create: {
            module: 'ut-log',
            args: {
                type: 'bunyan',
                name: 'bunyan_test',
                streams: [
                    {
                        level: 'trace',
                        stream: 'process.stdout'
                    }
                ]
            }
        }
    },
    sql: {
        create: 'ut-port-sql',
        init: 'init',
        properties: {
            config: {
                id: 'sql',
                logLevel: 'trace',
                db: {
                    user: 'switch',
                    password: 'switch',
                    server: '192.168.133.40',
                    database: 'utswitch_bakcellgpp'
                }
            },
            logFactory: {$ref: 'bunyan'}
        }
    }
}, {require: require}).then(function contextLoaded(context) {
    context.sql.start();
    console.log(context.sql.exec({
        $$: {
            mtid: '',
            opcode: ''
        },
        _sql: {
            process: 'return',
            sql: 'select * from Banks;select * from BankCommison'
        },
        a: 1, b: 2, c: 'martin', d: 3.14, e: "function() {console.log('sql port rockz!!!')}"
    }, function(err, result) {
        if (err)
            throw err;

        console.log(result);
    }));
}).done();
