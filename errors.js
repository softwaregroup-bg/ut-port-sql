var create = require('ut-error').define;

var PortSQL = create('PortSQL');
var NoConnection = create('NoConnection', PortSQL);
var MissingProcess = create('MissingProcess', PortSQL);
var NotImplemented = create('NotImplemented', PortSQL);

module.exports = {
    sql: function(cause) {
        return new PortSQL(cause);
    },
    noConnection: function(params) {
        return new NoConnection({message: 'No connection to SQL server', params: params});
    },
    missingProcess: function(process) {
        return new MissingProcess({message: 'Invalid SQL process parameter', params: {process: process}});
    },
    notImplemented: function(cause) {
        return new NotImplemented({message: 'SQL process not implemented', params: {process: cause}});
    }

};
