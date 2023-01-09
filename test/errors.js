module.exports = function error({registerErrors}) {
    return registerErrors(require('./errors.json'));
};
