const EOL = require('os').EOL;
var Transform = require('./transform');
let { getResultSetName } = require('./helpers');
const formatCellValue = (value) => {
    return value && `${value.replace(/\n|,/g, ' ')}`;
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
        this.options.canPushRow && this.stream.push((this.config.columns || []).map((col) => {
            return formatCellValue(col.transform ? col.transform(chunk[col.name], chunk) : chunk[col.name]);
        }).join(this.config.seperator || ',') + EOL);
    }
    onResultSet(chunk) {
        let { resultSetName, namedSet } = this.config;
        var cResultSetName = getResultSetName(chunk);
        this.options.canPushRow = ((namedSet && resultSetName && cResultSetName === resultSetName) || !namedSet);
    }
}
module.exports = CsvTransform;
