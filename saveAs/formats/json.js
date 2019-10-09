const Format = require('./format');

module.exports = class JsonFormat extends Format {
    constructor(config) {
        super(config);
        this.separator = '';
    }
    onResultSet(resultSet) {
        if (this.resultSet) {
            this.write(`${this.resultSet.single ? '' : ']'}},{`);
        } else {
            this.write('{');
        }
        this.write(`"${resultSet.resultSetName}": ${resultSet.single ? '' : '['}`);
        this.separator = '';
        this.resultSet = resultSet;
    }
    onRow(row) {
        if (!this.resultSet && !this.separator) this.write('[');
        this.write(this.separator + JSON.stringify(row));
        this.separator = ',';
    }
    onDone() {
        if (this.resultSet) {
            this.write(`${this.resultSet.single ? '' : ']'}}`);
        } else {
            this.write(']');
        }
    }
};
