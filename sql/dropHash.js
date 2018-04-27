module.exports = function() {
    return `IF OBJECT_ID('dbo.utSchemaHash') IS NOT NULL DROP FUNCTION dbo.utSchemaHash`;
};
