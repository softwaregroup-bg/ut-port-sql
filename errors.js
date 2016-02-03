var create = require('ut-error').define;

var PortSQL = create('PortSQL');
var NoConnection = create('NoConnection', PortSQL);
var NotReady = create('NotReady', PortSQL);
var MissingProcess = create('MissingProcess', PortSQL);
var NotImplemented = create('NotImplemented', PortSQL);
var UnexpectedType = create('UnexpectedType', PortSQL);
var UnexpectedColumnType = create('UnexpectedColumnType', PortSQL);

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
    }

};
