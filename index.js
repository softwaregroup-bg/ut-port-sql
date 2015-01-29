/**
 * @module SQL Port
 * @author UT Route Team
 * @description Microsoft SQL Server Port Module
 * @requires net
 * @requires mssql
 * @requires ut-bus/port
 * @requires util
 * @requires path
 * @requires fs
 */
(function(define) {define(function(require) {
    var net = require('net');
    var mssql = require('mssql');
    var Port = require('ut-bus/port');
    var util = require('util');
    var path = require('path');
    var fs = require('fs');

    function SqlPort() {
        Port.call(this);

        /**
         * @param {Object} config
         * @description Contains all SQL configuration data
         */
        this.config = null;
        /**
         * @function val
         * @description Empty validation method
         */
        this.val = null;
        /**
         * @function log
         * @description Empty logger method
         */
        this.log = null;
        /**
         * @param {Object} connection
         * @description SQL connection
         */
        this.connection = null;
        /**
         * @function retryInterval
         * @description passed to setInterval to ping the SQL server until a connection has been set
         */
        this.retryInterval = null;

        return this;
    }

    util.inherits(SqlPort, Port);

    /**
     * @function init
     * @description Extends the default Port.init() method
     */
    SqlPort.prototype.init = function init() {
        Port.prototype.init.apply(this, arguments);
    };

    /**
     * @function start
     * @description Extends the default Port.start() method and initializes the sql connection
     */
    SqlPort.prototype.start = function start() {
        Port.prototype.start.apply(this, arguments);

        this.connection = new mssql.Connection(this.config.db, function(err) {
            if (err) {
                this.retryInterval = setInterval(function() {
                    this.connection = new mssql.Connection(this.config.db, function(err) {
                        if (!err) {
                            this.stopRetryInterval();
                        }
                    }.bind(this));
                }.bind(this), 10000);
            }
        }.bind(this));
    };

    /**
     * @function exec
     * @description Handles SQL query execution
     * @param {Object} message
     * @param {Function} callback It's invoked after query execution
     */
    SqlPort.prototype.exec = function(message, callback) {

        if (!this.connection) {
            callback({
                _error: {
                    message: 'No connection to SQL server',
                    query: message._sql.sql
                }
            });
        }

        if (typeof this.val === 'function') {
            this.val(message);
        }

        var start = +new Date();
        var request = new mssql.Request(this.connection);
        request.query(message._sql.sql, function (err, result) {
            var end = +new Date();
            var execTime = end - start;
            console.log('\nQuery executed for ' + execTime.toString() + ' ms...\n');

            if (err) {
                callback({
                    _error: {
                        message: err.message,
                        query: message._sql.sql
                    }
                });
            } else {
                var response = {};
                Object.keys(message).forEach(function(value) {
                    if (value !== '_sql') {
                        response[value] = message[value];
                    }
                });
                if (result.length) {
                    switch (message._sql.process) {
                        case 'return':
                            Object.keys(result[0]).forEach(function (value) {
                                response[value] = result[0][value];
                            });
                            console.log(response);
                            break;
                        case 'returnConvert':
                            // TODO
                            break;
                        case 'json':
                            response.dataSet = result;
                            break;
                        case 'xls':
                            // TODO set XSL format string
                            break;
                        case 'csv':
                            // TODO set CSV format string
                            break;
                        case 'xml':
                            // TODO set XML format string
                            break;
                        case 'queueRows':
                            // TODO
                            break;
                        case 'processRows':
                            // TODO
                            break;
                    }
                }
                callback(null, response);
            }
        });
    };

    /**
     * @function stopRetryInterval
     * @description Stops retrying to connect to the sql database
     */
    SqlPort.prototype.stopRetryInterval = function() {
        clearInterval(this.retryInterval);
    };

    /**
     * @function schemaUpdate
     * @description Executes schemaUpdate for a specific implementation
     * @param {string} implementation The implementation for the schemaUpdate
     */
    SqlPort.prototype.schemaUpdate = function(implementation) {
        if (!this.connection)
            throw 'No SQL connection has been established...';

        if (!implementation.length)
            throw 'No implementation has been provided...';

        var schemaPath = path.resolve(process.cwd(), '../../../../impl/' + implementation + '/schema');
        var files = fs.readdirSync(schemaPath);
        if (files.length) {

            var sql = "";
            sql += "SELECT [type]";
            sql += "     , o.Name AS Name";
            sql += "     , c.text AS SQLScript";
            sql += " FROM";
            sql += "  dbo.syscomments c, dbo.sysobjects o";
            sql += " WHERE";
            sql += "  o.id = c.id";
            sql += "  AND xType IN ('V', 'P', 'FN','F','IF','SN','TF','TR','U')";
            sql += "  --AND user_name(objectproperty(o.ID, 'OwnerId')) = 'switch'";
            sql += "  AND objectproperty(o.ID, 'IsMSShipped') = 0";
            sql += " ORDER BY";
            sql += "  o.crdate";
            sql += ", c.id";
            sql += ", c.colid";

            var start = +new Date();
            var request = new mssql.Request(this.connection);
            request.query(sql, function (err, result) {
                var end = +new Date();
                var execTime = end - start;
                console.log('\nQuery executed for ' + execTime.toString() + ' ms...\n');

                if (err)
                    throw 'Error while executing SQL query...';

                if (result.length) {
                    var lookupSql = [];
                    for (var i = 0; i < result.length; i++) {
                        if (i === 0 || (result[i].Name !== result[i - 1].Name)) {
                            result[i].SQLScript = result[i].SQLScript;
                            lookupSql.push(result[i]);
                        } else {
                            lookupSql[lookupSql.length - 1].SQLScript += result[i].SQLScript;
                        }
                    }
                    var queries = [];
                    files = files.sort();
                    files.forEach(function(file, key) {
                        var filename = file.replace('.sql', '').split('$').pop();
                        var fileBody = fs.readFileSync(schemaPath + '/' + file).toString();
                        lookupSql.forEach(function(lSql, id) {
                            if (filename === lSql.Name) {
                                if (fileBody.trim() !== lSql.SQLScript.trim()) {
                                    queries.push(fileBody);
                                }
                            } else if (id === lookupSql.length - 1) {
                                queries.push(fileBody);
                            }
                        });
                    });
                    queries.forEach(function (query) {
                        var start = +new Date();
                        request.query(query.toString(), function(err, result) {
                            var end = +new Date();
                            var execTime = end - start;
                            console.log('\nQuery executed for ' + execTime.toString() + ' ms...\n');

                            if (err)
                                throw 'Error while executing schema update script...';

                            console.log('Schema update script executed successfully...');
                        });
                    });
                } else {
                    throw 'Empty schema update scripts from the database...';
                }
            });
        } else {
            throw 'Empty schema folder...';
        }
    };

    return SqlPort;

});}(typeof define === 'function' && define.amd ? define : function(factory) { module.exports = factory(require); }));