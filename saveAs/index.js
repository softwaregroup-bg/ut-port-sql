const through2 = require('through2');
const { getResultSetName } = require('./helpers');
const transforms = {
    json: require('./json'),
    jsonl: require('./jsonLines'),
    csv: require('./csv')
};
module.exports = function(request, saveAs) {
    const config = Object.assign({}, {
        namedSet: undefined,
        single: undefined
    }, typeof saveAs === 'object' ? saveAs : {});
    const filename = typeof saveAs === 'string' ? saveAs : saveAs.filename;
    const ext = filename.split('.').pop();
    const Transform = transforms[ext];
    if (!Transform) throw new Error('File type not supported');
    let transform;
    return request.pipe(through2({objectMode: true}, function(chunk, encoding, next) {
        if (config.namedSet === undefined) { // called only once to write object start literal
            config.namedSet = !!getResultSetName(chunk);
            transform = new Transform(this, config);
            transform.onStart();
        }
        if (config.namedSet && getResultSetName(chunk)) {
            transform.setOptions('single', !!chunk.single);
            transform.onResultSet(chunk);
        } else {
            transform.onRow(chunk);
        }
        next();
    }, function(next) {
        transform.onEnd();
        next();
    }));
};
