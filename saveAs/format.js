const { Readable } = require('readable-stream');
const asyncQueue = require('../asyncQueue');

class Stream extends Readable {
    _read() {}
}

module.exports = class Transform {
    constructor(port, request, config = {}) {
        this.config = config;
        const stream = new Stream();
        this.pipe = (...params) => stream.pipe(...params);
        this.write = chunk => stream.push(chunk);
        const { wrap } = asyncQueue();
        let transform;
        request.on('recordset', wrap(columns => {
            transform = port.getRowTransformer(columns);
        }));
        request.on('row', wrap(async row => {
            if (row.resultSetName) {
                this.onResultSet(row);
            } else if (typeof transform === 'function') {
                this.onRow(await transform(row));
            } else {
                this.onRow(row);
            }
        }));
        request.on('error', wrap(error => this.onError(error)));
        request.on('done', wrap(result => {
            this.onDone(result);
            this.write(null);
        }));
    }
    onResultSet() {}
    onRow() {}
    onError() {}
    onDone() {}
};
