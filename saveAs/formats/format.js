const EventEmitter = require('events');
module.exports = class Transform {
    constructor(config = {}) {
        this.config = config;
        const emitter = new EventEmitter();
        this.write = data => emitter.emit('data', data);
        this.on = (evt, cb) => {
            emitter.on(evt, cb);
            return this;
        };
    }

    onResultSet() {}
    onRow() {}
    onDone() {}
};
