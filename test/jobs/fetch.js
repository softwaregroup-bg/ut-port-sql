const fetch = (txt, expectedResult = []) => ({
    method: 'test.test.fetch',
    params: {
        'data.txt': txt
    },
    result(result, assert) {
        assert.strictSame(result.data,
            [
                {id: 1, txt: 'abcde'},
                {id: 2, txt: 'cdefg'}
            ].filter(({txt}) => [].concat(expectedResult).find(t => t === txt)),
            `test.test.fetch ${txt}`
        );
    }
});

module.exports = function test() {
    return {
        fetch: function(test, bus, run) {
            return run(test, bus, [
                fetch('abc', 'abcde'),
                fetch('abcde', 'abcde'),
                fetch('cde', ['abcde', 'cdefg']),
                fetch('abcdx', []),
                fetch('xyabc', []),
                fetch('abc cde', 'abcde')
            ]);
        }
    };
};
