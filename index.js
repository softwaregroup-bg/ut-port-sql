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
    this.retryTimeout = null;
    return this;
}

util.inherits(SqlPort, Port);

SqlPort.prototype.init = function init() {
    Port.prototype.init.apply(this, arguments);
    this.latency = this.counter && this.counter('average', 'lt', 'Latency');
};

SqlPort.prototype.connect = function connect() {
    this.connection && this.connection.close();
    this.connection = new mssql.Connection(this.config.db);
    return this.connection.connect()
        .then(this.loadSchema.bind(this))
        .then(this.updateSchema.bind(this))
        .then(this.linkSP.bind(this))
        .catch(function(err) {
            this.retryTimeout = setTimeout(this.connect.bind(this), 10000);
            this.log.error && this.log.error(err);
        }.bind(this));
};

SqlPort.prototype.start = function start() {
    Port.prototype.start.apply(this, Array.prototype.slice.call(arguments));
    this.bus && this.bus.importMethods(this.config, this.config.imports, undefined, this);
    return this.connect().then(function(result) {
        this.pipeExec(this.exec.bind(this), this.config.concurrency);
        return result;
    }.bind(this));
};
SqlPort.prototype.stop = function stop() {
    clearTimeout(this.retryTimeout);
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

SqlPort.prototype.checkConnection = function() {
    if (!this.connection) {
        throw errors.noConnection();
    }
};

/**
 * @function exec
 * @description Handles SQL query execution
 * @param {Object} message
 */
SqlPort.prototype.exec = function(message) {
    var $meta = (arguments.length && arguments[arguments.length - 1]);
    var methodName = ($meta && $meta.method);
    if (methodName) {
        var method = this.config[methodName];
        if (method instanceof Function) {
            return when.lift(method).apply(this, Array.prototype.slice.call(arguments));
        }
    }

    this.checkConnection();

    if (this.config.validate instanceof Function) {
        this.config.validate(message);
    }

    //var start = +new Date();
    var request = this.getRequest();
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

SqlPort.prototype.updateSchema = function(schema) {
    this.checkConnection();

    function getAlterStatement(statement) {
        return statement.trim().replace(/^CREATE /i, 'ALTER ');
    }

    function getCreateStatement(statement) {
        return statement.trim().replace(/^ALTER /i, 'CREATE ');
    }

    var self = this;
    var schemaPath = this.config.schema;
    if (!schemaPath) {
        return schema;
    }
    return when.promise(function(resolve, reject) {
        fs.readdir(schemaPath, function(err, files) {
            if (err) {
                reject(err);
            } else {
                var queries = [];
                files = files.sort();
                files.forEach(function(file) {
                    var objectName = file.toLowerCase().replace(/\.sql/, '').replace(/^[^\$]*\$/, ''); // remove "prefix$" and ".sql" suffix
                    var fileName = schemaPath + '/' + file;
                    var fileContent = fs.readFileSync(fileName).toString();
                    var createStatement = getCreateStatement(fileContent);
                    if (!schema.source[objectName]) {
                        queries.push({fileName: fileName, objectName: objectName, content: createStatement});
                    } else {
                        if (createStatement !== schema.source[objectName]) {
                            queries.push({fileName: fileName, objectName: objectName, content: getAlterStatement(fileContent)});
                        }
                    }
                });

                var request = self.getRequest();
                var currentFileName = '';
                var updated = [];
                when.reduce(queries, function(result, query) {
                        updated.push(query.objectName);
                        currentFileName = query.fileName;
                        return request.batch(query.content);
                    }, [])
                    .then(function() {
                        updated.length && self.log.info && self.log.info({message: updated, $meta: {opcode: 'updateSchema'}});
                        resolve(self.loadSchema());
                    })
                    .catch(function(error) {
                        error.fileName = currentFileName;
                        reject(error);
                    });
            }
        });
    });
};

SqlPort.prototype.execTemplate = function(template, params) {
    var self = this;
    return template.render(params).then(function(query) {
        return self.exec({query: query, process: 'json'})
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

SqlPort.prototype.getRequest = function() {
    return new mssql.Request(this.connection);
};

SqlPort.prototype.callSP = function(name, params) {
    var self = this;
    var outParams = [];

    params.forEach(function(param) {
        param.out && outParams.push(param.name);
    });

    function sqlType(def) {
        var type = mssql[def.type.toUpperCase()];
        if (def.size) {
            if (Array.isArray(def.size)) {
                type = type(def.size[0], def.size[1]);
            } else {
                type = (def.size === 'max') ? type(mssql.MAX) : type(def.size);
            }
        }
        return type;
    }

    return function callLinkedSP(msg) {
        var request = self.getRequest();
        request.multiple = true;
        params.forEach(function(param) {
            param.out ? request.output(param.name, sqlType(param.def), msg[param.name]) : request.input(param.name, sqlType(param.def), msg[param.name]);
        });
        return request.execute(name).then(function(result) {
            if (outParams.length) {
                result.push([outParams.reduce(function(prev, curr) {
                    prev[curr] = request.parameters[curr].value;
                    return prev;
                }, {})]);
            }
            return result;
        });
    };
};

SqlPort.prototype.linkSP = function(schema) {
    if (!this.config.linkSP) {
        return schema;
    }
    schema.bindings.forEach(function(binding) {
        if (!this.config[binding.name]) {
            this.config[binding.name] = this.callSP(binding.name, binding.params);
        }
    }.bind(this));
    return schema;
};

SqlPort.prototype.loadSchema = function() {
    var self = this;
    this.checkConnection();
    var request = this.getRequest();
    var sql = `SELECT
            RTRIM([type]) [type],
            SCHEMA_NAME(o.schema_id) [namespace],
            o.Name AS [name],
            SCHEMA_NAME(o.schema_id) + '.' + o.Name AS [full],
            c.text AS [source]
        FROM
            dbo.syscomments c,
            sys.objects o
        WHERE
            o.object_id = c.id AND
            o.type IN ('V', 'P', 'FN','F','IF','SN','TF','TR','U') AND
            user_name(objectproperty(o.object_id, 'OwnerId')) = '${this.config.db.user}' AND
            objectproperty(o.object_id, 'IsMSShipped') = 0
        ORDER BY
            o.create_date, c.id, c.colid`;
    return request.query(sql).then(function(result) {
        return result.reduce(function(prev, cur) {
            var type = prev[cur.type] || (prev[cur.type] = {});
            type[cur.full] = (type[cur.full] || '') + cur.source;
            prev.source[cur.full.toLowerCase()] = (prev.source[cur.full] || '') + cur.source;
            if (self.config.linkSP && cur.type === 'P') {
                var parserSP = require('./parsers/mssqlSP');
                var procedure = parserSP.parse(cur.source);
                procedure && (procedure.type === 'procedure') && (prev.bindings.push(procedure));
            }
            return prev;
        }, {source: {}, bindings: []});
    });
};

module.exports = SqlPort;
