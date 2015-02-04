define([
    'intern!bdd',
    'intern/chai!assert',
    'intern/chai!expect',
    'intern/dojo/node!../index',
    'intern/dojo/node!ut-validate',
    'intern/dojo/node!ut-log'
], function(bdd, assert, expect, SQL, validators, Logger) {

    bdd.describe('SQL', function() {
        bdd.before(function() {
            console.log('Start SQL protocol testing...\n');
        });
        bdd.after(function() {
            console.log('\nFinished testing SQL protocol...\n');
        });
        bdd.it('SQL connection: should succeed immediately', function() {
            var sql = new SQL({
                id: 'sql',
                logLevel: 'trace',
                db: {
                    user: 'switch',
                    password: 'switch',
                    server: '192.168.133.40',
                    database: 'utswitch_bakcellgpp'
                }
            });
            sql.start();
        });
        bdd.it('SQL query(process=return): should succeed', function() {
            var sql = new SQL({
                id: 'sql',
                logLevel: 'trace',
                db: {
                    user: 'switch',
                    password: 'switch',
                    server: '192.168.133.40',
                    database: 'utswitch_bakcellgpp'
                }
            });
            sql.start();
            sql.exec({
                $$: {
                    mtid: 'request',
                    opcode: 'account.banks'
                },
                process: 'return',
                query: 'select * from Banks',
                a: 1, b: 2, c: 'martin', d: 3.14, e: "function() {console.log('sql port rockz!!!')}"
            }, function(err, result) {
                if (err)
                    throw err;
                assert.deepEqual(Object.keys(result), [
                    '$$',
                    'process',
                    'query',
                    'a',
                    'b',
                    'c',
                    'd',
                    'e',
                    'ID',
                    'BankCode',
                    'Commison',
                    'State',
                    'Name',
                    'Description',
                    'swiftcode'
                ]);
            });
        });
        bdd.it('SQL query(process=json): should succeed', function() {
            var sql = new SQL({
                id: 'sql',
                logLevel: 'trace',
                db: {
                    user: 'switch',
                    password: 'switch',
                    server: '192.168.133.40',
                    database: 'utswitch_bakcellgpp'
                }
            });
            sql.start();
            sql.exec({
                $$: {
                    mtid: 'request',
                    opcode: 'account.banks'
                },
                process: 'json',
                query: 'select * from Banks',
                a: 1, b: 2, c: 'martin', d: 3.14, e: "function() {console.log('sql port rockz!!!')}"
            }, function(err, result) {
                if (err)
                    throw err;
                expect(typeof result.dataSet).to.be.equal('object');
            });
        });
    });
});
