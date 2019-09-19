const Format = require('./format');

module.exports = class JsonL extends Format {
    constructor(request, config) {
        super(request, config);
        if (typeof this.config.lineSeparator !== 'string') this.config.lineSeparator = '\r\n';
    }
    onRow(row) {
        super.onRow(row);
        const line = JSON.stringify({[this.resultSetName]: row});
        this.stream.push(`${line}${this.config.lineSeparator}`);
    }
};
