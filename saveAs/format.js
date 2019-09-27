const { Readable } = require('readable-stream');

const queue = ({
    concurrency = 1
} = {}) => {
    let running = 0;
    const tasks = [];

    const run = async task => {
        running++;
        await task();
        running--;
        if (tasks.length > 0) run(tasks.shift());
    };

    const push = task => running < concurrency ? run(task) : tasks.push(task);

    const wrap = fn => (...params) => push(() => fn(...params));

    return {
        push,
        wrap
    };
};

class Stream extends Readable {
    _read() {}
}

module.exports = class Transform {
    constructor(port, request, config = {}) {
        this.config = config;
        const stream = new Stream();
        this.pipe = (...params) => stream.pipe(...params);
        this.write = chunk => stream.push(chunk);
        const { wrap } = queue();
        let transformRow;
        request.on('recordset', wrap(columns => {
            transformRow = port.getRowTransformer(columns);
        }));
        request.on('row', wrap(async row => {
            if (row.resultSetName) {
                this.onResultSet(row);
            } else if (transformRow) {
                this.onRow(await transformRow(row));
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
