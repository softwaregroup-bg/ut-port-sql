module.exports = function() {
    return `IF OBJECT_ID('dbo.utSchemaHash') IS NOT NULL SELECT dbo.utSchemaHash() hash`;
};
