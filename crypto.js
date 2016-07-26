var crypto = require('crypto');
var defPassword = 'some password';

module.exports = {
    encrypt: (text, algorithm, password) => {
        var cipher = crypto.createCipher(algorithm, password || defPassword);

        return new Promise((resolve, reject) => {
            var encrypted = '';
            cipher.on('readable', () => {
                var data = cipher.read();
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
        var decipher = crypto.createDecipher(algorithm, password || defPassword);

        return new Promise((resolve, reject) => {
            var decrypted = '';
            decipher.on('readable', () => {
                var data = decipher.read();
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
