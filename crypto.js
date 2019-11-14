'use strict';
const crypto = require('crypto');
const defPassword = '12345678901234567890123456789012';
const defIV = '1234567890123456';

module.exports = {
    encrypt: (text, algorithm, password, iv) => {
        const cipher = crypto.createCipheriv(algorithm, password || defPassword, iv || defIV);

        return new Promise((resolve, reject) => {
            let encrypted = '';
            cipher.on('readable', () => {
                const data = cipher.read();
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
    decrypt: (text, algorithm, password, iv) => {
        const decipher = crypto.createDecipheriv(algorithm, password || defPassword, iv || defIV);

        return new Promise((resolve, reject) => {
            let decrypted = '';
            decipher.on('readable', () => {
                const data = decipher.read();
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
    },
    hmac: key => value => crypto.createHmac('sha256', Buffer.from(key, 'hex')).update(value).digest()
};
