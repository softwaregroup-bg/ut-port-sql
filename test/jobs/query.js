module.exports = function test() {
    return {
        query: function(test, bus, run) {
            return run(test, bus, [{
                method: 'test.query',
                params: {
                    query: 'SELECT 1 AS test',
                    process: 'json'
                },
                result(result, assert) {
                    assert.ok(Array.isArray(result.dataSet), 'result returned');
                    assert.equal(result.dataSet[0].test, 1, 'result correctness checked');
                }
            }]);
        }
    };
};
