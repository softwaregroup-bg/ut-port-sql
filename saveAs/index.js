const path = require('path');
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
        fs.mkdir(path.dirname(outputFilePath), {recursive: true}, err => {
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
    const fileStream = writer.pipe(fs.createWriteStream(outputFilePath));

    return new Promise((resolve, reject) => {
        let replied = false;

        const abort = err => {
            if (replied) return;
            replied = true;

            port.log.error?.(err);
            request.cancel();
            fs.unlink(outputFilePath, err => err && port.log.error?.(err));
            reject(port.errors['portSQL.exportError'](err));
        };

        const reply = () => {
            if (replied) return;
            replied = true;

            fileStream.on('finish', () => {
                try {
                    if (saveAs.stream) {
                        const pipe = fs.createReadStream(outputFilePath).pipe(
                            crypto.createDecipheriv(algorithm, key, iv)
                        );
                        pipe.on('error', error => port.log.error?.(error));
                        return resolve(Object.assign(pipe, {
                            toJSON: () => ({path: outputFilePath}),
                            httpResponse: () => ({
                                type: formatter.mime,
                                header: ['content-disposition', `attachment; filename="${path.basename(saveAs.stream)}"`]
                            })
                        }));
                    } else return resolve({outputFilePath, encryption: {algorithm, key: key.toString('hex'), iv: iv.toString('hex')}});
                } catch (e) {
                    port.log.error?.(e);
                    fs.unlink(outputFilePath, err => err && port.log.error?.(err));
                    reject(port.errors['portSQL.exportError'](e));
                }
            });

            formatter.onDone();
            writer.end();
        };

        const wrap = fn => async(...params) => {
            try {
                await fn(...params);
            } catch (e) {
                abort(e);
            }
        };

        writer.on('error', abort);
        fileStream.on('error', abort);

        formatter.on('data', data => {
            writer.write(data);
        });

        let transform;
        request.on('recordset', wrap(columns => {
            transform = port.getRowTransformer(columns);
        }));

        request.on('row', wrap(async row => {
            if (row.resultSetName) {
                formatter.onResultSet(row);
            } else {
                if (typeof transform === 'function') {
                    if (transform.constructor.name === 'AsyncFunction') {
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

        request.on('error', abort);
        request.on('done', () => reply());
        request.execute(name);
    });
};
