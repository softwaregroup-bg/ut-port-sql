const { Readable } = require('readable-stream');
class Stream extends Readable {
    _read() {}
}
module.exports = class Transform {
    constructor(request, config) {
        this.config = config;
        this.stream = new Stream();
        let transform = row => row;
        request.on('recordset', columns => {
            if (columns.resultSetName) return;
            const middleware = [];
            Object.entries(columns).forEach(([key, {type: {declaration}}]) => {
                if (declaration === 'varbinary') {
                    // if (self.cbc && isEncrypted(columns[column])) {
                    //     middleware.push(record => {
                    //         if (record[key]) { // value is not null
                    //             record[key] = self.cbc.decrypt(record[key]);
                    //         }
                    //     });
                    // }
                } else if (declaration === 'xml') {
                    // middleware.push(record => {
                    //     if (record[key]) { // value is not null
                    //         return new Promise(function(resolve, reject) {
                    //             xmlParser.parseString(record[key], function(err, result) {
                    //                 if (err) {
                    //                     reject(sqlPortErrors['portSQL.wrongXmlFormat']({
                    //                         xml: record[key]
                    //                     }));
                    //                 } else {
                    //                     record[key] = result;
                    //                     resolve();
                    //                 }
                    //             });
                    //         });
                    //     }
                    // });
                }
                if (/\.json$/i.test(key)) {
                    // middleware.push(record => {
                    //     record[key.substr(0, key.length - 5)] = record[key] ? JSON.parse(record[key]) : record[key];
                    //     delete record[key];
                    // });
                };
                if (middleware.length) {
                    // transform = async row => {
                    //     for (let i = 0, n = middleware.length; i < n; i += 1) {
                    //         await middleware[i](row);
                    //     }
                    // };
                }
                this.onRecordSet();
            });
        });
        request.on('row', row => {
            if (row.resultSetName) {
                this.resultSetName = row.resultSetName;
                this.single = row.single;
                return;
            }
            this.onRow(transform(row));
        });
        request.on('error', error => this.onError(error));
        request.on('done', result => this.onDone(result));
    }
    onRecordSet() {}
    onRow() {}
    onError() {}
    onDone() {
        this.stream.push(null);
    }
};
