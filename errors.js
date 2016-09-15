var create = require('ut-error').define;

var PortSQL = create('PortSQL');
var NoConnection = create('NoConnection', PortSQL);
var NotReady = create('NotReady', PortSQL);
var MissingProcess = create('MissingProcess', PortSQL);
var NotImplemented = create('NotImplemented', PortSQL);
var UnexpectedType = create('UnexpectedType', PortSQL);
var UnexpectedColumnType = create('UnexpectedColumnType', PortSQL);
var InvalidView = create('InvalidView', PortSQL);
var InvalidResultSetOrder = create('InvalidResultSetOrder', PortSQL);
var DuplicateResultSetName = create('DuplicateResultSetName', PortSQL);
var SingleResultExpected = create('SingleResultExpected', PortSQL);
var WrongXmlFormat = create('WrongXmlFormat', PortSQL);
var RetryFailedSchemas = create('RetryFailedSchemas', PortSQL);

module.exports = {
    sql: function(cause) {
        return new PortSQL(cause);
    },
    noConnection: function(params) {
        return new NoConnection({message: 'No connection to SQL server', params: params});
    },
    notReady: function(params) {
        return new NotReady({message: 'The connection is not ready', params: params});
    },
    missingProcess: function(process) {
        return new MissingProcess({message: 'Invalid SQL process parameter', params: {process: process}});
    },
    notImplemented: function(cause) {
        return new NotImplemented({message: 'SQL process not implemented', params: {process: cause}});
    },
    unexpectedType: function(params) {
        return new UnexpectedType({message: 'Unexpected type', params: params});
    },
    unexpectedColumnType: function(params) {
        return new UnexpectedColumnType({message: 'Unexpected column type', params: params});
    },
    invalidView: function(params) {
        return new InvalidView({message: 'Invalid view', params: params});
    },
    invalidResultSetOrder: function(params) {
        return new InvalidResultSetOrder({message: 'Invalid resultset order', params: params});
    },
    duplicateResultSetName: function(params) {
        return new DuplicateResultSetName({message: 'Duplicate resultset name', params: params});
    },
    singleResultExpected: function(params) {
        return new SingleResultExpected({message: 'Expected single or no result', params: params});
    },
    wrongXmlFormat: function(params) {
        return new WrongXmlFormat({message: 'Wrong XML format', params: params});
    },
    retryFailedSchemas: function(params) {
        return new RetryFailedSchemas({message: 'Retries exceeded for failed schemas', params: params});
    }
};
