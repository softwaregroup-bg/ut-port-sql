'use strict';
module.exports = ({defineError, getError, fetchErrors}) => {
    if (!getError('portSQL')) {
        const PortSQL = defineError('portSQL', null, 'Port SQL generic');

        defineError('noConnection', PortSQL, 'No connection to SQL server');
        defineError('parserError', PortSQL, 'Sql parser error');
        defineError('notReady', PortSQL, 'The connection is not ready');
        defineError('missingProcess', PortSQL, 'Invalid or missing resultset processing mode');
        defineError('notImplemented', PortSQL, 'Specified resultset processing mode is not implemented yet');
        defineError('unexpectedType', PortSQL, 'Unexpected parameter type');
        defineError('unexpectedColumnType', PortSQL, 'Unexpected column type');
        defineError('invalidView', PortSQL, 'Invalid view, cannot refresh');
        defineError('invalidResultSetOrder', PortSQL, 'Invalid resultset order');
        defineError('duplicateResultSetName', PortSQL, 'Duplicate resultset name');
        defineError('singleResultExpected', PortSQL, 'Expected single or no result');
        defineError('wrongXmlFormat', PortSQL, 'Wrong XML format in result');
        defineError('retryFailedSchemas', PortSQL, 'Retries exceeded for failed schemas');
        defineError('noRowsExpected', PortSQL, 'No rows were expected in the result');
        defineError('singleResultsetExpected', PortSQL, 'Single resultset expected');
        defineError('oneRowExpected', PortSQL, 'Exactly one row was expected in the result');
        defineError('maxOneRowExpected', PortSQL, 'Maximum one row was expected in the result');
        defineError('minOneRowExpected', PortSQL, 'Minimum one row was expected in the result');
        defineError('absolutePath', PortSQL, 'Absolute path error');
        defineError('invalidFileLocation', PortSQL, 'Writing outside of base directory is forbidden');
    }

    return fetchErrors('portSQL');
};
