'use strict';
const crypto = require('crypto');
const defPassword = '12345678901234567890123456789012';
const defIV = '1234567890123456';
const zeroes = Buffer.alloc(16);

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
        const iv = Buffer.alloc(16);
        crypto.randomFillSync(iv);
        if (typeof key === 'string') {
            key = Buffer.from(key, 'hex');
        }
        const enc = crypto.createCipheriv('aes-256-cbc', key, iv);
        const dec = crypto.createDecipheriv('aes-256-cbc', key, iv);
        dec.update(iv);

        function pad(s) {
            s = Buffer.from(s);
            return Buffer.concat([s, Buffer.alloc((16 - s.length % 16) % 16), ' ']);
        }

        const encrypt = value => Buffer.concat([enc.update(crypto.randomFillSync(iv)), enc.update(pad(value))]);
        const decrypt = value => dec.update(Buffer.concat([value, iv.slice(value.length % 16)])).toString('utf8', 32).trim();
        const encryptStable = value => {
            const cipher = crypto.createCipheriv('aes-256-cbc', key, zeroes);
            return Buffer.concat([cipher.update(value), cipher.final()]);
        };
        const decryptStable = value => {
            const decipher = crypto.createDecipheriv('aes-256-cbc', key, zeroes);
            return decipher.update(value).toString('utf8') + decipher.final('utf8');
        };

        return {
            encrypt: (value, stable) => stable ? encryptStable(value) : encrypt(value),
            decrypt: (value, stable) => stable ? decryptStable(value) : decrypt(value)
        };
    },
    hmac: key => value => crypto.createHmac('sha256', Buffer.from(key, 'hex')).update(value).digest()
};
