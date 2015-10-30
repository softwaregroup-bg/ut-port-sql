var mssql = require('mssql');
var Port = require('ut-bus/port');
var util = require('util');
var fs = require('fs');
var when = require('when');
var errors = require('./errors');

function SqlPort() {
    Port.call(this);
    this.config = {
        id: null,
        logLevel: '',
        type: 'sql'
    };
    this.connection = null;
    this.retryInterval = null;
    return this;
}

util.inherits(SqlPort, Port);

SqlPort.prototype.init = function init() {
    Port.prototype.init.apply(this, arguments);
    this.latency = this.counter && this.counter('average', 'lt', 'Latency');
};

/**
 * @function start
 * @description Extends the default Port.start() method and initializes the sql connection
 */
SqlPort.prototype.start = function start() {
    Port.prototype.start.apply(this, Array.prototype.slice.call(arguments));
    this.bus && this.bus.importMethods(this.config, this.config.imports, undefined, this);

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
    this.pipeExec(this.exec.bind(this), this.config.concurrency);
};

SqlPort.prototype.stop = function stop() {
    this.queue.push();
    this.connection.close();
    Port.prototype.stop.apply(this, Array.prototype.slice.call(arguments));
};

function setPathProperty(object, fieldName, fieldValue) {
    var path = fieldName.split('.');
    fieldName = path.pop();
    path.forEach(function(name) {
        if (name) {
            if (!(object[name] instanceof Object)) {
                object[name] = {};
            }
            object = object[name];
        }
    });
    object[fieldName] = fieldValue;
}

/**
 * @function exec
 * @description Handles SQL query execution
 * @param {Object} message
 */
SqlPort.prototype.exec = function(message) {
    var $meta = (arguments.length && arguments[arguments.length - 1]);
    var methodName = ($meta && $meta.opcode);
    if (methodName) {
        var method = this.config[methodName];
        if (method instanceof Function) {
            return when.lift(method).apply(this, Array.prototype.slice.call(arguments));
        }
    }

    if (!this.connection) {
        return when.reject(errors.noConnection());
    }

    if (this.config.validate instanceof Function) {
        this.config.validate(message);
    }

    //var start = +new Date();
    var request = new mssql.Request(this.connection);
    return when.promise(function(resolve, reject) {
        request.query(message.query, function(err, result) {
            //var end = +new Date();
            //var execTime = end - start;
            //todo record execution time
            if (err) {
                reject(errors.sql(err));
            } else {
                $meta.mtid = 'response';
                if (result && result.length) {
                    if (message.process === 'return') {
                        Object.keys(result[0]).forEach(function(value) {
                            setPathProperty(message, value, result[0][value]);
                        });
                        resolve(message);
                    } else if (message.process === 'json') {
                        message.dataSet = result;
                        resolve(message);
                    } else if (message.process === 'xls') { //todo
                        reject(errors.notImplemented(message.process));
                    } else if (message.process === 'xml') { //todo
                        reject(errors.notImplemented(message.process));
                    } else if (message.process === 'csv') { //todo
                        reject(errors.notImplemented(message.process));
                    } else if (message.process === 'processRows') { //todo
                        reject(errors.notImplemented(message.process));
                    } else if (message.process === 'queueRows') { //todo
                        reject(errors.notImplemented(message.process));
                    } else {
                        reject(errors.missingProcess(message.process));
                    }
                }
            }
        });
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
 */
SqlPort.prototype.schemaUpdate = function() {
    if (!this.connection) {
        return when.reject(errors.noConnection());
    }

    var schemaPath = this.config.schema;
    var files = fs.readdirSync(schemaPath);
    var self = this;

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
            return when.reject(errors.sql(err));
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
            queries.forEach(function(query) { //todo execute queries in sequence, not in parallel and return promise
                request.query(query.toString(), function(err) {
                    err && self.log && self.log.trace && self.log.error(err);
                });
            });
        } else {
            return when.reject(errors.sql(err));
        }
    });
};

SqlPort.prototype.execTemplate = function(template, params) {
    var self = this;
    return template.render(params).then(function(query) {
        return self.exec({query:query, process: 'json'})
            .then(function(result) {
                    return result && result.dataSet;
                });
    });
};

SqlPort.prototype.execTemplateRow = function(template, params) {
    return this.execTemplate(template, params).then(function(data) {
        var result = (data && data[0]) || {};
        if (result._errorCode && parseInt(result._errorCode) !== 0) {
            // throw error if _errorCode is '', undefined, null, number (different than 0) or string (different than '0', '00', etc.)
            throw errors.sql({
                code: result._errorCode || -1,
                message: result._errorMessage || 'sql error'
            });
        } else {
            return result;
        }
    });
};

SqlPort.prototype.execTemplateRows = function(template, params) {
    return this.execTemplate(template, params).then(function(data) {
        var result = data || [{}];
        if (result[0]._errorCode && parseInt(result[0]._errorCode) !== 0) {
            // throw error if _errorCode is '', undefined, null, number (different than 0) or string (different than '0', '00', etc.)
            throw errors.sql({
                code: result._errorCode || -1,
                message: result._errorMessage || 'sql error'
            });
        } else {
            return result;
        }
    });
};

module.exports = SqlPort;
