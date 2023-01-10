module.exports = function test() {
    return {
        deadlock: function(test, bus, run) {
            return run(test, bus, [{
                method: 'test.test.deadlock',
                params: {},
                result(result, assert) {
                    assert.ok(result, 'deadlock retried');
                }
            }]);
        }
    };
};
