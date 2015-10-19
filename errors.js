var create = require('errno').custom.createError;

var PortSQL = create('PortSQL');
var NoConnection = create('NoConnection', PortSQL);
var MissingProcess = create('MissingProcess', PortSQL);
var NotImplemented = create('NotImplemented', PortSQL);

module.exports = {
    sql: function(cause) {
        return new PortSQL('SQL error', cause);
    },
    noConnection: function(cause) {
        return new NoConnection('No connection to SQL server', cause);
    },
    missingProcess: function(cause) {
        return new MissingProcess('Invalid SQL process parameter', cause);
    },
    notImplemented: function(cause) {
        return new NotImplemented('SQL process not implemented', cause);
    }

};
