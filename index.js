/**
 * @module sql
 * @author UT Route Team
 * @description SQL module
 * @requires mssql
 * @requires when
 */
var sql = require('mssql');
var when = require('when');

/**
 * @class SQL
 * @description Constructor method initiating the database connection
 * @param {Object} params Configuration paramters to connect to SQL database
 * $returns {Object} The current instance
 */
function SQL(params) {
    /**
     * @function val
     * @description Empty validation method
     */
    this.val = params.validator || null;
    /**
     * @function log
     * @description Empty logger method
     */
    this.log = params.logger || null;
    /**
     * @param {Object} connection
     * @description SQL connection
     */
    this.connection = new sql.Connection(params, function(err) {
        if (err) throw err;
    });

    return this;
}
/**
 * @function exec
 * @description Handles SQL query execution
 * @param {Object} params
 * @returns {Promise} Returns a promise to be handled after being executed
 */
SQL.prototype.exec = function(params) {

    if (this.val !== null) {
        this.val(params);
    }

    var request = new sql.Request(this.connection);

    return when.promise(function (resolve, reject) {
        request.query(params._sql.sql, function (err, recordset) {
            if (err) {
                reject({
                    _error: {
                        message: err.message,
                        query: params._sql.sql
                    }
                });
            } else {
                switch (params._sql.process) {
                    case 'return':
                        var response = {}
                        Object.keys(params).forEach(function(value) {
                            if (value !== '_sql') {
                                response[value] = params[value];
                            }
                        });
                        if (recordset.length) {
                            Object.keys(recordset[0]).forEach(function(value) {
                                response[value] = recordset[0][value];
                            });
                        }
                        resolve(response);
                        break;
                }
            }
        });
    });
};

module.exports = SQL;