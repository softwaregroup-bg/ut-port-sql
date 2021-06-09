const through2 = require('through2');
const fs = require('fs');
let { getResultSetName } = require('./helpers');
var transforms = {
    json: require('./json'),
    csv: require('./csv'),
    xlsx: require('./xlsx')
};
module.exports = function(request, saveAs) {
    let spParams = saveAs.spParams;
    var config = Object.assign({}, {
        namedSet: undefined,
        single: undefined
    }, typeof saveAs === 'object' ? saveAs : {});
    var filename = typeof saveAs === 'string' ? saveAs : saveAs.filename;
    var ext = filename.split('.').pop();
    let Transform = transforms[ext];
    if (!Transform) throw new Error('File type not supported');
    var transform;
    return new Promise((resolve, reject) => {
        let flush = async function(next) {
            await transform.onEnd();
            next();
        };
        if (ext === 'xlsx') {
            flush = async function(next) {
                await transform.onEnd();
                next();
                resolve();
            };
        }
        let stream = request.pipe(through2({objectMode: true}, function(chunk, encoding, next) {
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
        }, flush));
        if (ext === 'xlsx') {
            stream.on('error', function(error) {
                return reject(error);
            });
        } else {
            let ws = fs.createWriteStream(config.filename);
            stream.pipe(ws);
            ws.on('finish', () => {
                resolve();
            });
            ws.on('error', (error) => {
                reject(error);
            });
        }
        request.execute(spParams);
    });
     
};
