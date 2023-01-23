module.exports = function test() {
    return {
        stack: function(test, bus, run) {
            return run(test, bus, [{
                method: 'test.test.error',
                params: {},
                error(error, assert) {
                    assert.same(error.type, 'test.error', 'Error type');
                    assert.match(error.message, 'Test error message', 'Error message');
                    assert.match(error.message, '750-test.test.error.sql:4:1', 'Error procedure');
                    assert.match(error.message, '750-test.test.stack.sql:8:1', 'Called procedure');
                }
            }]);
        }
    };
};
