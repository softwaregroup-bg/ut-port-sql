module.exports = function test() {
    return {
        params: function(test, bus, run) {
            return run(test, bus, [{
                method: 'test.test.params',
                params: {
                    obj: {
                        a: 1
                    },
                    tt: {
                        content: {
                            b: 1
                        }
                    }
                },
                result({obj, tt}, assert) {
                    assert.strictSame(JSON.parse(obj.obj), {a: 1}, 'obj returned');
                    assert.strictSame(JSON.parse(tt[0].content), {b: 1}, 'tt returned');
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
