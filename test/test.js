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
    try {
        /*
        Localhost
        var local = require(require('path').resolve(process.cwd() + '/local.json'));
        var mssql = require('node-sqlserver-unofficial');
        mssql.query(local.conn, 'select * from Account', function (err, results) {
            if (err) {
                console.log(err)
            } else {
                console.log(results)
            }
        });
        */

        context.sql.start();
        context.sql.exec({
            _sql: {
                process: 'json',
                sql: 'select * from Banks'
            },
            a: 1, b: 2, c: 'martin', d: 3.14, e: "function() {console.log('sql port rockz!!!')}"
        }, function(err, result) {
            if (err)
                throw err;
        });

    } catch (e) {
        console.log(e);
    }
}).done();
