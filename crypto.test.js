const cbc = require('./crypto').cbc(require('crypto').randomBytes(32).toString('hex'));
const name = 'منفذ النومان';

const tap = require('tap');

tap.test('encrypt', async assert => {
    assert.same(cbc.decrypt(cbc.encrypt(name, true), true), name, 'encrypt decrypt stable');
    assert.same(cbc.decrypt(cbc.encrypt(name)), name, 'encrypt decrypt random');
});
