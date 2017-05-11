var create = require('ut-error').define;
var PortSQL = create('portSQL');

module.exports = {
    sql: PortSQL,
    noConnection: create('noConnection', PortSQL, 'No connection to SQL server'),
    parserError: create('parserError', PortSQL, 'Sql parser error'),
    notReady: create('notReady', PortSQL, 'The connection is not ready'),
    missingProcess: create('missingProcess', PortSQL, 'Invalid or missing resultset processing mode'),
    notImplemented: create('notImplemented', PortSQL, 'Specified resultset processing mode is not implemented yet'),
    unexpectedType: create('unexpectedType', PortSQL, 'Unexpected parameter type'),
    unexpectedColumnType: create('unexpectedColumnType', PortSQL, 'Unexpected column type'),
    invalidView: create('invalidView', PortSQL, 'Invalid view, cannot refresh'),
    invalidResultSetOrder: create('invalidResultSetOrder', PortSQL, 'Invalid resultset order'),
    duplicateResultSetName: create('duplicateResultSetName', PortSQL, 'Duplicate resultset name'),
    singleResultExpected: create('singleResultExpected', PortSQL, 'Expected single or no result'),
    wrongXmlFormat: create('wrongXmlFormat', PortSQL, 'Wrong XML format in result'),
    retryFailedSchemas: create('retryFailedSchemas', PortSQL, 'Retries exceeded for failed schemas'),
    noRowsExpected: create('noRowsExpected', PortSQL, 'No rows were expected in the result'),
    singleResultsetExpected: create('singleResultsetExpected', PortSQL, 'Single resultset expected'),
    oneRowExpected: create('oneRowExpected', PortSQL, 'Exactly one row was expected in the result'),
    maxOneRowExpected: create('maxOneRowExpected', PortSQL, 'Maximum one row was expected in the result'),
    minOneRowExpected: create('minOneRowExpected', PortSQL, 'Minimum one row was expected in the result')
};
