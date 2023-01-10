module.exports = function test() {
    return {
        private: function(test, bus, run) {
            return run(test, bus, [{
                method: 'test._test.private',
                params: {},
                error(error, assert) {
                    assert.equal(error.type, 'bus.methodNotFound', 'SP is private because it starts with _');
                }
            }]);
        }
    };
};
