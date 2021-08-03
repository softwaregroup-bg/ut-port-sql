const EOL = require('os').EOL;
const Transform = require('./transform');
const { getResultSetName } = require('./helpers');
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
        const { columns } = this.config;
        this.stream.push((columns || []).map((col) => formatCellValue(col.displayName)).join(this.config.seperator || ',') + EOL);
    }

    onRow(chunk) {
        this.options.canPushRow && this.stream.push((this.config.columns || []).map((col) => {
            return formatCellValue(col.transform ? col.transform(chunk[col.name], chunk) : chunk[col.name]);
        }).join(this.config.seperator || ',') + EOL);
    }

    onResultSet(chunk) {
        const { resultSetName, namedSet } = this.config;
        const cResultSetName = getResultSetName(chunk);
        this.options.canPushRow = ((namedSet && resultSetName && cResultSetName === resultSetName) || !namedSet);
    }
}
module.exports = CsvTransform;
