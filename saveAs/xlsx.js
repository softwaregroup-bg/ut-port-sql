var Transform = require('./transform');
let { getResultSetName } = require('./helpers');
let Excel = require('exceljs');
const fs = require('fs');
class XlsxTransform extends Transform {
    constructor(stream, config) {
        super(stream, config);
        this.options = {
            canPushRow: !config.namedSet,
            xlsxFilename: config.filename
        };
        let ws = fs.createWriteStream(config.filename);
        this.workbook = new Excel.stream.xlsx.WorkbookWriter({stream: ws});
        this.worksheet = this.workbook.addWorksheet(config.sheetName || 'Sheet 1');
    }
    onStart() {
        let { columns } = this.config;
        this.worksheet.columns = (columns || []).map(col => ({
            key: col.name,
            header: col.displayName
        }));
    }
    onRow(chunk) {
        if (this.options.canPushRow) {
            chunk = this.config.onRow ? this.config.onRow(chunk) : chunk;
            this.worksheet.addRow(chunk).commit();
        }
    }
    async onEnd() {
        await this.workbook.commit();
        // await this.workbook.xlsx.writeFile(this.options.xlsxFilename);
    }
    onResultSet(chunk) {
        let { resultSetName, namedSet } = this.config;
        var cResultSetName = getResultSetName(chunk);
        this.options.canPushRow = ((namedSet && resultSetName && cResultSetName === resultSetName) || !namedSet);
    }
}
module.exports = XlsxTransform;
