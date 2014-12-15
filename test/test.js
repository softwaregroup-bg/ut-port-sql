var SQL = require('../');
//var mocha = require('mocha');
//var expect = require('chai').expect;
//var assert = require('chai').assert;


return false;
describe('Query Positive', function() {
    it('Running a successful query', function() {
        // test validation
        expect(sql.val({
            _sql: {
                process: 'return',
                sql: 'select * from Account'
            },
            what: 'v1',
            ever: 'v2'
        })).to.be.equal(true);
        // test result
        sql.exec({
            _sql: {
                process: 'return',
                sql: 'select * from Account'
            },
            what: 'v1',
            ever: 'v2'
        }).then(function(msg) {
            msg.deepEqual({ what: 'v1', ever: 'v2', number: 1, number2: 2 });
        }).catch(function(err) {
            err.deepEqual({ what: 'v1', ever: 'v2', number: 1, number2: 2 });
        });
    })
});

describe('Query Negative', function() {
    it('Running unsuccessful query', function() {
        sql.exec({
            _sql: {
                process: 'return',
                sql: 'select * from Account'
            },
            what: 'v1',
            ever: 'v2'
        }).then(function(msg) {
            msg.notDeepEqual({ what: 'v1s', ever: 'v2', number: 1, number2: 2 });
        }).catch(function(err) {
            console.log(err);
        });
    });
});