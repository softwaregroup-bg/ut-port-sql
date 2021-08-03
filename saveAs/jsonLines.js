const Transform = require('./transform');
const { getResultSetName } = require('./helpers');

module.exports = class JsonLinesTransform extends Transform {
    constructor(stream, config) {
        if (!config.lineSeparator || typeof config.lineSeparator !== 'string') {
            config.lineSeparator = '\r\n';
        }
        super(stream, config);
    }

    onRow(chunk) {
        const jsonString = JSON.stringify({[this.currentResultset]: chunk});
        const line = `${jsonString}${this.config.lineSeparator}`;
        this.stream.push(line);
    }

    onResultSet(chunk) {
        this.currentResultset = getResultSetName(chunk);
    }
};
