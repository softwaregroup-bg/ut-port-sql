const Format = require('./format');

module.exports = class JsonLFormat extends Format {
    constructor(port, request, config) {
        super(port, request, config);
        this.resultSet = {};
        if (typeof this.config.lineSeparator !== 'string') this.config.lineSeparator = '\r\n';
    }
    onResultSet(resultSet) {
        this.resultSet = resultSet;
    }
    onRow(row) {
        const line = JSON.stringify({[this.resultSet.resultSetName]: row});
        this.write(`${line}${this.config.lineSeparator}`);
    }
};
