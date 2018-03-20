var Transform = require('./transform');
let { getResultSetName } = require('./helpers');
class CsvTransform extends Transform {
    constructor(stream, config) {
        super(stream, config);
        this.options = {
            canPushRow: !config.namedSet,
            newLine: '\n'
        };
    }
    onStart() {
        let { columns } = this.config;
        this.stream.push((columns || []).map((col) => `"${col.displayName}"`).join(this.config.seperator || ',') + this.options.newLine);
    }
    onRow(chunk) {
        this.options.canPushRow && this.stream.push((this.config.columns || []).map((col) => {
            return ['"', col.transform ? col.transform(chunk[col.name], chunk) : chunk[col.name], '"'].join('');
        }).join(this.config.seperator || ',') + this.options.newLine);
    }
    onResultSet(chunk) {
        let { resultSetName, namedSet } = this.config;
        var cResultSetName = getResultSetName(chunk);
        this.options.canPushRow = ((namedSet && resultSetName && cResultSetName === resultSetName) || !namedSet);
    }
}
module.exports = CsvTransform;
