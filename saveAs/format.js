const { Readable } = require('readable-stream');

class Stream extends Readable {
    _read() {}
}

module.exports = class Transform {
    constructor(port, request, config = {}) {
        this.config = config;
        const stream = new Stream();
        this.pipe = (...params) => stream.pipe(...params);
        this.write = chunk => stream.push(chunk);
        let transform;
        request.on('recordset', columns => {
            transform = port.getRowTransformer(columns);
        });
        request.on('row', row => {
            if (row.resultSetName) {
                this.onResultSet(row);
            } else {
                if (typeof transform === 'function') {
                    if (transform.constructor.name === 'AsyncFunction') {
                        request.pause();
                        return (async() => {
                            await transform(row);
                            this.onRow(row);
                            request.resume();
                        })();
                    } else {
                        transform(row);
                    }
                }
                this.onRow(row);
            }
        });
        request.on('error', error => this.onError(error));
        request.on('done', result => {
            this.onDone(result);
            this.write(null);
        });
    }
    onResultSet() {}
    onRow() {}
    onError() {}
    onDone() {}
};
