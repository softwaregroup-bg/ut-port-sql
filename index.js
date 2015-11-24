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
    this.bus && this.bus.importMethods(this.config, this.config.imports, undefined, this);
    return Port.prototype.start.apply(this, Array.prototype.slice.call(arguments))
        .then(this.connect.bind(this))
        .then(function(result) {
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
                if (message.process === 'return') {
                    if (result && result.length) {
                        Object.keys(result[0]).forEach(function(value) {
                            setPathProperty(message, value, result[0][value]);
                        });
                    }
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
    var schemas = Array.isArray(this.config.schema) ? this.config.schema : [{path:this.config.schema}];
    if (!schemas) {
        return schema;
    }

    return when.reduce(schemas, function(prev, schemaConfig) {// visit each schema folder
            return when.promise(function(resolve, reject) {
                fs.readdir(schemaConfig.path, function(err, files) {
                    if (err) {
                        reject(err);
                    } else {
                        var queries = [];
                        files = files.sort();
                        files.forEach(function(file) {
                            var objectName = file.replace(/\.sql/i, '').replace(/^[^\$]*\$/, ''); // remove "prefix$" and ".sql" suffix
                            var objectId = objectName.toLowerCase();
                            schemaConfig.linkSP && prev.push(objectId);
                            var fileName = schemaConfig.path + '/' + file;
                            var fileContent = fs.readFileSync(fileName).toString();
                            var createStatement = getCreateStatement(fileContent);
                            if (schema.source[objectId] === undefined) {
                                queries.push({fileName: fileName, objectName: objectName, objectId: objectId, content: createStatement});
                            } else {
                                if (schema.source[objectId].length && (createStatement !== schema.source[objectId])) {
                                    queries.push({fileName: fileName, objectName: objectName, objectId: objectId, content: getAlterStatement(fileContent)});
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
                                resolve(prev);
                            })
                            .catch(function(error) {
                                error.fileName = currentFileName;
                                reject(error);
                            });
                    }
                });
            }, []);
        },[])
        .then(function(objectList) {
            return self.loadSchema(objectList);
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

    params && params.forEach(function(param) {
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
        params && params.forEach(function(param) {
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
    if (schema.parseList.length) {
        var parserSP = require('./parsers/mssqlSP');
        schema.parseList.forEach(function(source) {
            var binding = parserSP.parse(source);
            if (binding && binding.type === 'procedure' && !this.config[binding.name]) {
                this.config[binding.name] = this.callSP(binding.name, binding.params);
            }
        }.bind(this));
    }
    return schema;
};

SqlPort.prototype.loadSchema = function(objectList) {
    var self = this;
    if ((Array.isArray(this.config.schema) && !this.config.schema.length) || !this.config.schema) {
        return {source: {}, parseList: []};
    }

    this.checkConnection();
    var request = this.getRequest();
    var sql = `SELECT
            o.create_date,
            c.id,
            c.colid,
            RTRIM(o.[type]) [type],
            SCHEMA_NAME(o.schema_id) [namespace],
            o.Name AS [name],
            SCHEMA_NAME(o.schema_id) + '.' + o.Name AS [full],
            CASE o.[type]
              WHEN 'SN' THEN 'DROP SYNONYM [' + SCHEMA_NAME(o.schema_id) + '].[' + o.Name +
                             '] CREATE SYNONYM [' + SCHEMA_NAME(o.schema_id) + '].[' + o.Name + '] FOR ' +  s.base_object_name
              ELSE c.text
            END AS [source]

        FROM
            sys.objects o
        LEFT JOIN
            dbo.syscomments c on o.object_id = c.id
        LEFT JOIN
            sys.synonyms s on s.object_id = o.object_id
        WHERE
            o.type IN ('V', 'P', 'FN','F','IF','SN','TF','TR','U') AND
            user_name(objectproperty(o.object_id, 'OwnerId')) = USER_NAME() AND
            objectproperty(o.object_id, 'IsMSShipped') = 0
        UNION ALL
        SELECT 0,0,0,'S',name,NULL,NULL,NULL FROM sys.schemas WHERE principal_id = USER_ID()
        ORDER BY
            1, 2, 3`;
    return request.query(sql).then(function(result) {
        return result.reduce(function(prev, cur) {
            cur.namespace = cur.namespace && cur.namespace.toLowerCase();
            cur.full = cur.full && cur.full.toLowerCase();
            if (cur.source) {
                prev.source[cur.full] = (prev.source[cur.full] || '') + (cur.source || '');
            } else if (cur.full) {
                prev.source[cur.full] = '';
            } else {
                prev.source[cur.namespace] = '';
            }
            if ((cur.type === 'P') && (cur.colid === 1) && (self.config.linkSP || (objectList && objectList.indexOf(cur.full) >= 0))) {
                prev.parseList.push(cur.source);
            }
            return prev;
        }, {source: {}, parseList: []});
    });
};

module.exports = SqlPort;
