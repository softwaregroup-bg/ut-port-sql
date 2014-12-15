(function(define){ define(function(require) {
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
     * @tutorial sql
     * @description Constructor method initiating the database connection
     * @param {Object} config Configuration paramters to connect to SQL database
     * $returns {Object} The current instance
     */

    function SQL(config, validator, logger) {
        /**
         * @function val
         * @description Empty validation method
         */
        this.val = validator;
        /**
         * @function log
         * @description Empty logger method
         */
        this.log = logger;
        /**
         * @param {Object} connection
         * @description SQL connection
         */
        this.connection = new sql.Connection(config, function(err) {
            if (err) throw err;
        });

        return this;
    }
    /**
     * @function exec
     * @description Handles SQL query execution
     * @param {Object} data
     * @returns {Promise} Returns a promise to be handled after being executed
     */
    SQL.prototype.exec = function(data) {

        if (typeof this.val === 'function') {
            this.val(data);
        }

        var request = new sql.Request(this.connection);

        return when.promise(function (resolve, reject) {
            request.query(data._sql.sql, function (err, recordset) {
                if (err) {
                    reject({
                        _error: {
                            message: err.message,
                            query: data._sql.sql
                        }
                    });
                } else {
                    switch (data._sql.process) {
                        case 'return':
                            var response = {}
                            Object.keys(data).forEach(function(value) {
                                if (value !== '_sql') {
                                    response[value] = data[value];
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

    return SQL;

});})(typeof define === 'function' && define.amd ?  define : function(factory){ module.exports = factory(require); });