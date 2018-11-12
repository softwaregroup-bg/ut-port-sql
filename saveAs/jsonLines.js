var Transform = require('./transform');
let { getResultSetName } = require('./helpers');

module.exports = class JsonLinesTransform extends Transform {
    constructor(stream, config) {
        if (!config.lineSeparator || typeof config.lineSeparator !== 'string') {
            config.lineSeparator = '\r\n';
        }
        super(stream, config);
    }
    onRow(chunk) {
        let jsonString = JSON.stringify({[this.currentResultset]: chunk});
        let line = `${jsonString}${this.config.lineSeparator}`;
        this.stream.push(line);
    }
    onResultSet(chunk) {
        this.currentResultset = getResultSetName(chunk);
    }
};
