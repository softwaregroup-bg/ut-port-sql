const Format = require('./format');
const EOL = require('os').EOL;
const format = val => String(val).replace(/\n|,/g, ' ');

module.exports = class CsvFormat extends Format {
    constructor(port, request, config) {
        super(port, request, config);
        if (!Array.isArray(this.config.columns)) this.config.columns = [];
        if (typeof this.config.separator !== 'string') this.config.separator = ',';
    }
    writeHeader() {
        this.write(this.config.columns.map(col => format(col.displayName)).join(this.config.separator) + EOL);
    }
    onResultSet(resultSet) {
        if (this.canPushRow) {
            this.canPushRow = false;
        } else {
            this.canPushRow = resultSet.resultSetName === this.config.resultSetName;
        }
        if (this.canPushRow) this.writeHeader();
    }
    onRow(row) {
        if (this.canPushRow === undefined) {
            this.writeHeader();
            this.canPushRow = true;
        }
        if (this.canPushRow) {
            this.write(this.config.columns.map(c => format(c.transform ? c.transform(row[c.name], row) : row[c.name])).join(',') + EOL);
        }
    }
};
