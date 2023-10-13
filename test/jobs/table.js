module.exports = function test() {
    return {
        table: function(test, bus, run) {
            return run(test, bus, [{
                method: 'test.test.table',
                params: {
                    table: 'test.test'
                },
                result(result, assert) {
                    assert.strictSame(result, {
                        table: [
                            {column: 'add1', type: 'varchar', length: '20', scale: null, default: "('-')"},
                            {column: 'add2', type: 'decimal', length: '10', scale: 5, default: null},
                            {column: 'column1', type: 'varchar', length: '40', scale: null, default: null},
                            {column: 'column2', type: 'varchar', length: '30', scale: null, default: null},
                            {column: 'column3', type: 'decimal', length: '10', scale: 5, default: null},
                            {column: 'column4', type: 'int', length: null, scale: null, default: null},
                            {column: 'column5', type: 'int', length: null, scale: null, default: null},
                            {column: 'id', type: 'tinyint', length: null, scale: null, default: null},
                            {column: 'txt', type: 'varbinary', length: '48', scale: null, default: null}
                        ]
                    }, 'expected altered table columns');
                }
            },
            {
                name: 'securityViolation',
                method: 'test.test.params',
                $meta: {frontEnd: 'fake'},
                params: {},
                error(error, assert) {
                    assert.equal(error.type, 'test.securityViolation');
                }
            }]);
        }
    };
};
