const path = require('path');
const fsplus = require('fs-plus');
const fs = require('fs');
const formats = {
    jsonl: require('./formats/jsonl'),
    json: require('./formats/json'),
    csv: require('./formats/csv')
};
const crypto = require('crypto');

module.exports = async(port, request, { saveAs }, name) => {
    const config = typeof saveAs === 'string' ? { filename: saveAs } : saveAs;
    if (path.isAbsolute(config.filename)) throw this.errors['portSQL.absolutePath']();

    const baseDir = path.join(port.bus.config.workDir, 'ut-port-sql', 'export');
    const outputFilePath = path.resolve(baseDir, config.filename);
    if (!outputFilePath.startsWith(baseDir)) throw port.errors['portSQL.invalidFileLocation']();

    await new Promise((resolve, reject) => {
        fsplus.makeTree(path.dirname(outputFilePath), err => {
            if (!err || err.code === 'EEXIST') {
                return resolve();
            }
            return reject(err);
        });
    });

    request.stream = true;
    const ext = config.filename.split('.').pop();
    const Format = formats[ext] || formats.jsonl;
    const formatter = new Format(config);
    const algorithm = 'aes-256-cbc';
    const key = crypto.randomBytes(32);
    const iv = crypto.randomBytes(16);
    const writer = crypto.createCipheriv(algorithm, key, iv);
    writer.pipe(fs.createWriteStream(outputFilePath));

    return new Promise((resolve, reject) => {
        let replied = false;
        const reply = err => {
            if (replied) return;
            replied = true;
            writer.end();
            if (!err) {
                try {
                    formatter.onDone();
                    if (saveAs.stream) {
                        return resolve(Object.assign(fs.createReadStream(outputFilePath).pipe(
                            crypto.createDecipheriv(algorithm, key, iv)
                        ), {
                            toJSON: () => ({path: outputFilePath}),
                            httpResponse: () => ({
                                type: formatter.mime,
                                header: ['content-disposition', `attachment; filename="${path.basename(saveAs.stream)}"`]
                            })
                        }));
                    } else return resolve({outputFilePath, encryption: {algorithm, key, iv}});
                } catch (e) {
                    err = e;
                }
            } else {
                request.cancel();
            }
            port.log.error && port.log.error(err);
            try {
                fs.unlinkSync(outputFilePath);
            } catch (e) {
                port.log.error && port.log.error(e);
            }
            reject(port.errors['portSQL.exportError'](err));
        };

        const wrap = fn => async(...params) => {
            try {
                await fn(...params);
            } catch (e) {
                reply(e);
            }
        };

        let transform;

        formatter.on('data', data => {
            writer.write(data);
        });

        request.on('recordset', wrap(columns => {
            transform = port.getRowTransformer(columns);
        }));

        request.on('row', wrap(async row => {
            if (row.resultSetName) {
                formatter.onResultSet(row);
            } else {
                if (typeof transform === 'function') {
                    if (transform.constructor.name !== 'AsyncFunction') {
                        request.pause();
                        await transform(row);
                        request.resume();
                    } else {
                        transform(row);
                    }
                }
                formatter.onRow(row);
            }
        }));

        request.on('error', reply);

        request.on('done', () => reply());

        request.execute(name);
    });
};
