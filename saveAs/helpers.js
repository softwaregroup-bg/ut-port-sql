function getResultSetName(chunk) {
    const keys = Object.keys(chunk);
    return keys.length > 0 && keys[0].toLowerCase() === 'resultsetname' ? chunk[keys[0]] : null;
}
module.exports = {
    getResultSetName
};
