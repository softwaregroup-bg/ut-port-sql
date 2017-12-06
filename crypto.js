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
    }
};
