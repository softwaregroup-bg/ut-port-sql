module.exports = function(hash) {
    return `CREATE FUNCTION dbo.utSchemaHash() RETURNS VARCHAR(64) AS BEGIN RETURN '${hash}' END`;
};
