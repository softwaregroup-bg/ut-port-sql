//var mocha = require('mocha');
//var expect = require('chai').expect;
//var assert = require('chai').assert;
var wire = require('wire');

m = wire({
    winston : {
        create: {
            module: 'ut-log',
            args: {
                type: 'winston',
                name: 'winston_test',
                dependencies: [],
                transports: {
                    'file': {
                        filename: './winstonTest.log',
                        level: 'trace'
                    },
                    'console': {
                        colorize: 'true',
                        level: 'trace'
                    }
                }
            }
        }
    },
    sql: {
        create: {
            module: 'ut-port-sql',
            args: [
                {
                    user: 'switch',
                    password: 'switch',
                    server: '192.168.133.40',
                    database: 'G_MFSP3_2'
                },
                require('ut-validate').get('joi').validateSql,
                {$ref: 'winston'}
            ]
        }
    }
}, {require : require});

m.then(function(c) {
    var sql = c.sql;
    console.log(sql.val({
        _sql: {
            process: 'return',
            sql: 'select * from Account'
        },
        what: 'v1',
        ever: 'v2'
    }));

    sql.exec({
        _sql: {
            process: 'return',
            sql: 'select * from Account'
        },
        what: 'v1',
        ever: 'v2'
    }).then(function(msg) {
        console.log(msg);
    }).catch(function(err) {
        console.log(msg);
    });

}).otherwise(function(error){
    err = error;
    console.log(err)
});