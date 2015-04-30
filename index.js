var mssql = require('mssql');
var Port = require('ut-bus/port');
var util = require('util');
var path = require('path');
var fs = require('fs');
var when = require('when');

function SqlPort(config, validator, logger) {
    Port.call(this);

    /**
     * @param {Object} config
     * @description Contains all SQL configuration data
     */
    this.config = config ? config : {};
    /**
     * @function val
     * @description Empty validation method
     */
    this.val = validator ? validator : null;
    /**
     * @function log
     * @description Empty logger method
     */
    this.log = logger ? logger : {};
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
/* istanbul ignore next */
SqlPort.prototype.init = function init() {
    Port.prototype.init.apply(this, arguments);
};

/**
 * @function start
 * @description Extends the default Port.start() method and initializes the sql connection
 */
SqlPort.prototype.start = function start() {
    Port.prototype.start.apply(this, arguments);
    this.bus && this.bus.importMethods(this.config, this.config.imports);

    this.connection = new mssql.Connection(this.config.db, function(err) {
        /* istanbul ignore if */
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
    this.pipeExec(this.exec.bind(this));
};

/* istanbul ignore next */
function _mergeResultAndResponse(response, fieldName, fieldValue) {
    var names = fieldName.split('.');
    if (names[0] === '$$' && (names.length === 1 || names[1] === 'callback')) {
        response.$$.mtid = 'error';
        response.$$.errorCode = '123';
        response.$$.errorMessage = 'Returned invalid sql field $$ or callback!';
    }
    names.reverse();
    var resp3 = {};
    if (names.length === 1) {
        response[fieldName] = fieldValue;
    }else {
        for (var i = 0; i < names.length; i += 1) {
            var resp2 = {};
            var currName = names[i];
            if (i === 0) {
                resp3[names[0]] = fieldValue;
            } else {
                resp2[currName] = resp3;
                if ((i + 1) < names.length) {
                    resp3 = resp2;
                }
            }
            if ((i + 1) === names.length) {
                if (response[currName]) {
                    response[currName][names[(i - 1)]] = resp3[names[(i - 1)]];
                } else {
                    response[currName] = resp3;
                }
            }
        }
    }
    return response;
}

/**
 * @function exec
 * @description Handles SQL query execution
 * @param {Object} message
 * @param {Function} callback It's invoked after query execution
 */
SqlPort.prototype.exec = function(message, callback) {

    var methodName = (message && message.$$ && message.$$.opcode);
    if (methodName) {
        var method = this.config[methodName];
        if (method instanceof Function) {
            when(method.apply(this, [message]))
                .then(function(res) {
                    callback(null, res);
                })
                .catch(function(err) {
                    callback(err);
                });
            return;
        }
    }

    if (!this.connection) {
        message.$$.mtid = 'error';
        message.$$.errorCode = '500';
        message.$$.errorMessage = 'No connection to SQL server';
        callback(message);
    }

    if (typeof this.val === 'function') {
        this.val(message);
    }

    //var start = +new Date();
    var request = new mssql.Request(this.connection);
    request.query(message.query, function(err, result) {
        //var end = +new Date();
        //var execTime = end - start;
        //todo record execution time

        if (err) {
            message.$$.mtid = 'error';
            message.$$.errorCode = '500';
            message.$$.errorMessage = err.message;
            callback(message);
        } else {
            var response = {};
            Object.keys(message).forEach(function(value) {
                response[value] = message[value];
            });
            response.$$.mtid = 'response';
            if (result.length) {
                if (message.process) {
                    switch (message.process) {
                        case 'return':
                            Object.keys(result[0]).forEach(function(value) {
                                response = _mergeResultAndResponse(response, value, result[0][value]);
                            });
                            break;
                        case 'returnConvert':
                            // TODO
                            break;
                        case 'json':
                            response.dataSet = result;
                            break;
                        case 'xls':
                            /* istanbul ignore next */
                            // TODO set XSL format string
                            break;
                        case 'csv':
                            /* istanbul ignore next */
                            // TODO set CSV format string
                            break;
                        case 'xml':
                            /* istanbul ignore next */
                            // TODO set XML format string
                            break;
                        case 'queueRows':
                            /* istanbul ignore next */
                            // TODO
                            break;
                        case 'processRows':
                            /* istanbul ignore next */
                            // TODO
                            break;
                        default:
                            message.$$.mtid = 'error';
                            message.$$.errorCode = '500';
                            message.$$.errorMessage = 'message.process unknown';
                            callback(message);
                            break;
                    }
                } else {
                    message.$$.mtid = 'error';
                    message.$$.errorCode = '500';
                    message.$$.errorMessage = 'message.process undefined';
                    callback(message);
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
/* istanbul ignore next */
SqlPort.prototype.schemaUpdate = function(implementation) {
    if (!this.connection) {
        throw 'No SQL connection has been established...';
    }
    if (!implementation.length) {
        throw 'No implementation has been provided...';
    }

    var schemaPath = path.resolve(process.cwd(), '../../../../impl/' + implementation + '/schema');
    var files = fs.readdirSync(schemaPath);
    if (files.length) {

        //todo use execTemplate
        var sql = '';
        sql += 'SELECT [type]';
        sql += '     , o.Name AS Name';
        sql += '     , c.text AS SQLScript';
        sql += ' FROM';
        sql += '  dbo.syscomments c, dbo.sysobjects o';
        sql += ' WHERE';
        sql += '  o.id = c.id';
        sql += '  AND xType IN (\'V\', \'P\', \'FN\',\'F\',\'IF\',\'SN\',\'TF\',\'TR\',\'U\')';
        sql += '  --AND user_name(objectproperty(o.ID, \'OwnerId\')) = \'switch\'';
        sql += '  AND objectproperty(o.ID, \'IsMSShipped\') = 0';
        sql += ' ORDER BY';
        sql += '  o.crdate';
        sql += ', c.id';
        sql += ', c.colid';

        var request = new mssql.Request(this.connection);
        request.query(sql, function(err, result) {

            if (err) {
                throw 'Error while executing SQL query...';
            }

            if (result.length) {
                var lookupSql = [];
                for (var i = 0; i < result.length; i += 1) {
                    if (i === 0 || (result[i].Name !== result[i - 1].Name)) {
                        lookupSql.push(result[i]);
                    } else {
                        lookupSql[lookupSql.length - 1].SQLScript += result[i].SQLScript;
                    }
                }
                var queries = [];
                files = files.sort();
                files.forEach(function(file) {
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
                queries.forEach(function(query) {
                    var start = +new Date();
                    request.query(query.toString(), function(err) {
                        var end = +new Date();
                        var execTime = end - start;
                        console.log('\nQuery executed for ' + execTime.toString() + ' ms...\n');

                        if (err) {
                            throw 'Error while executing schema update script...';
                        }

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

SqlPort.prototype.execTemplate = function(template, params) {
    var self = this;
    return template.render(params)
        .then(function(query) {
            return when.promise(function(resolve, reject) {
                self.exec({query:query, process: 'json'}, function(err, result) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(result && result.dataSet);
                    }

                });
            });
        }
    );
};

SqlPort.prototype.execTemplateRow = function(template, params) {
    return this.execTemplate(template, params).then(function(result) {
        return result && result[0];
    });
};

module.exports = SqlPort;
