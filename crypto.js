'use strict';
const crypto = require('crypto');
const defPassword = 'some password';

module.exports = {
    encrypt: (text, algorithm, password) => {
        let cipher = crypto.createCipher(algorithm, password || defPassword);

        return new Promise((resolve, reject) => {
            let encrypted = '';
            cipher.on('readable', () => {
                let data = cipher.read();
                if (data) {
                    encrypted += data.toString('hex');
                }
            });
            cipher.on('end', () => {
                resolve(encrypted);
            });

            cipher.write(text);
            cipher.end();
        });
    },
    decrypt: (text, algorithm, password) => {
        let decipher = crypto.createDecipher(algorithm, password || defPassword);

        return new Promise((resolve, reject) => {
            let decrypted = '';
            decipher.on('readable', () => {
                let data = decipher.read();
                if (data) {
                    decrypted += data.toString('utf8');
                }
            });
            decipher.on('end', () => {
                resolve(decrypted);
            });

            decipher.write(text, 'hex');
            decipher.end();
        });
    },
    hash: content => crypto.createHash('sha256').update(content).digest('hex'),
    cbc: key => {
        var iv = Buffer.alloc(16);
        crypto.randomFillSync(iv);
        if (typeof key === 'string') {
            key = Buffer.from(key, 'hex');
        }
        var enc = crypto.createCipheriv('aes-256-cbc', key, iv);
        var dec = crypto.createDecipheriv('aes-256-cbc', key, iv);
        dec.update(iv);

        function pad(s) {
            return s + ' '.repeat(16 - s.length % 16);
        }

        return {
            encrypt: value => Buffer.concat([enc.update(crypto.randomFillSync(iv)), enc.update(pad(value))]),
            decrypt: value => dec.update(Buffer.concat([value, iv])).toString('utf8', 32).trim()
        };
    }
};
