const EOL = require('os').EOL;
var Transform = require('./transform');
let { getResultSetName } = require('./helpers');
const formatCellValue = (value) => {
    return String(value).replace(/\n|,/g, ' ');
};
class CsvTransform extends Transform {
    constructor(stream, config) {
        super(stream, config);
        this.options = {
            canPushRow: !config.namedSet
        };
    }
    onStart() {
        let { columns } = this.config;
        this.stream.push((columns || []).map((col) => formatCellValue(col.displayName)).join(this.config.seperator || ',') + EOL);
    }
    onRow(chunk) {
        let row;
        if (this.options.canPushRow) {
            chunk = this.config.onRow ? this.config.onRow(chunk) : chunk;
            row = (this.config.columns || []).map((col) => {
                return formatCellValue(col.transform ? col.transform(chunk[col.name], chunk) : chunk[col.name] || '');
            });
        }
        this.options.canPushRow && row && this.stream.push(row.join(this.config.seperator || ',') + EOL);
    }
    onResultSet(chunk) {
        let { resultSetName, namedSet } = this.config;
        var cResultSetName = getResultSetName(chunk);
        this.options.canPushRow = ((namedSet && resultSetName && cResultSetName === resultSetName) || !namedSet);
    }
}
module.exports = CsvTransform;
