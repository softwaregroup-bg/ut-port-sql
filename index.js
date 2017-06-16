var mssql = require('ut-mssql');
var Port = require('ut-bus/port');
var util = require('util');
var fs = require('fs');
var when = require('when');
var errors = require('./errors');
var crypto = require('./crypto');
var utError = require('ut-error');
var mssqlQueries = require('./sql');
var xml2js = require('xml2js');
var uuid = require('uuid');
var xmlParser = new xml2js.Parser({explicitRoot: false, charkey: 'text', mergeAttrs: true, explicitArray: false});
var xmlBuilder = new xml2js.Builder({headless: true});
const AUDIT_LOG = /^[\s+]{0,}--ut-audit-params$/m;
const CORE_ERROR = /^[\s+]{0,}EXEC \[?core]?\.\[?error]?$/m;
const CALL_PARAMS = /^[\s+]{0,}DECLARE @callParams XML$/m;
const VAR = /\$\{([^}]*)\}/g;
const ROW_VERSION_INNER_TYPE = 'BINARY';

function changeRowVersionType(field) {
    if (field && (field.type.toUpperCase() === 'ROWVERSION' || field.type.toUpperCase() === 'TIMESTAMP')) {
        field.type = ROW_VERSION_INNER_TYPE;
        field.length = 8;
    }
}

function SqlPort() {
    Port.call(this);
    this.config = {
        id: null,
        logLevel: '',
        type: 'sql',
        createTT: false,
        retry: 10000,
        tableToType: {},
        skipTableType: [],
        paramsOutName: 'out',
        doc: false
    };
    this.super = {};
    this.connection = null;
    this.retryTimeout = null;
    return this;
}

function fieldSource(column) {
    return (column.column + '\t' + column.type + '\t' + (column.length === null ? '' : column.length) + '\t' + (column.scale === null ? '' : column.scale)).toLowerCase();
}

util.inherits(SqlPort, Port);

SqlPort.prototype.init = function init() {
    Port.prototype.init.apply(this, arguments);
    this.latency = this.counter && this.counter('average', 'lt', 'Latency');
};

SqlPort.prototype.connect = function connect() {
    this.connection && this.connection.close();
    this.connectionReady = false;

    return Promise.resolve()
        .then(() => {
            if (this.config.db.cryptoAlgorithm) {
                return crypto.decrypt(this.config.db.password, this.config.db.cryptoAlgorithm);
            }
            return false;
        })
        .then((r) => {
            this.config.db.password = (r || this.config.db.password);
            return this.tryConnect();
        })
        .then(this.refreshView.bind(this, true))
        .then(this.loadSchema.bind(this, null))
        .then(this.updateSchema.bind(this))
        .then(this.refreshView.bind(this, false))
        .then(this.linkSP.bind(this))
        .then(this.doc.bind(this))
        .then((v) => { this.connectionReady = true; return v; })
        .catch((err) => {
            try { this.connection.close(); } catch (e) {};
            if (this.config.retry) {
                this.retryTimeout = setTimeout(this.connect.bind(this), 10000);
                this.log.error && this.log.error(err);
            } else {
                this.log.fatal && this.log.fatal(err);
                return Promise.reject(err);
            }
        });
};

SqlPort.prototype.start = function start() {
    this.bus && this.bus.importMethods(this.config, this.config.imports, undefined, this);

    this.config.imports && this.config.imports.forEach(impl => {
        if (Array.isArray(this.config[impl + '.skipTableType'])) {
            this.config.skipTableType = this.config.skipTableType.concat(this.config[impl + '.skipTableType']);
        };
    });

    if (Array.isArray(this.config.createTT)) {
        Object.assign(this.config.tableToType, this.config.createTT.reduce(function(obj, tableName) {
            obj[tableName.toLowerCase()] = true;
            return obj;
        }, {}));
    }
    return Port.prototype.start.apply(this, Array.prototype.slice.call(arguments))
        .then(this.connect.bind(this))
        .then(function(result) {
            this.pipeExec(this.exec.bind(this), this.config.concurrency);
            return result;
        }.bind(this));
};

SqlPort.prototype.stop = function stop() {
    clearTimeout(this.retryTimeout);
    // this.queue.push();
    this.connectionReady = false;
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

SqlPort.prototype.checkConnection = function(checkReady) {
    if (!this.connection) {
        throw errors.noConnection({
            server: this.config.db && this.config.db.server,
            database: this.config.db && this.config.db.database
        });
    }
    if (checkReady && !this.connectionReady) {
        throw errors.notReady({
            server: this.config.db && this.config.db.server,
            database: this.config.db && this.config.db.database
        });
    }
};

/**
 * @function exec
 * @description Handles SQL query execution
 * @param {Object} message
 */
SqlPort.prototype.exec = function(message) {
    var $meta = (arguments.length && arguments[arguments.length - 1]);
    $meta.debug = !!this.bus.config.debug;
    var methodName = ($meta && $meta.method);
    if (methodName) {
        var parts = methodName.match(/^([^[]*)(\[[0+?^]?])?$/);
        var modifier;
        if (parts) {
            methodName = parts[1];
            modifier = parts[2];
        }
        var method = this.config[methodName];
        if (!method) {
            methodName = methodName.split('/', 2);
            method = methodName.length === 2 && this.config[methodName[1]];
        }
        if (method instanceof Function) {
            return when.lift(method).apply(this, Array.prototype.slice.call(arguments))
            .then(result => {
                switch (modifier) {
                    case '[]':
                        if (result && result.length === 1) {
                            return result[0];
                        } else {
                            throw errors.singleResultsetExpected();
                        }
                    case '[^]':
                        if (result && result.length === 0) {
                            return null;
                        } else {
                            throw errors.noRowsExpected();
                        }
                    case '[0]':
                        if (result && result.length === 1 && result[0] && result[0].length === 1) {
                            return result[0][0];
                        } else {
                            throw errors.oneRowExpected();
                        }
                    case '[?]':
                        if (result && result.length === 1 && result[0] && result[0].length <= 1) {
                            return result[0][0];
                        } else {
                            throw errors.maxOneRowExpected();
                        }
                    case '[+]':
                        if (result && result.length === 1 && result[0] && result[0].length >= 1) {
                            return result[0];
                        } else {
                            throw errors.minOneRowExpected();
                        }
                    default:
                        return result;
                }
            });
        }
    }

    this.checkConnection(true);

    if (this.config.validate instanceof Function) {
        this.config.validate(message);
    }

    // var start = +new Date();
    var debug = this.isDebug();
    var request = this.getRequest();
    return when.promise(function(resolve, reject) {
        request.query(message.query, function(err, result) {
            // var end = +new Date();
            // var execTime = end - start;
            // todo record execution time
            if (err) {
                debug && (err.query = message.query);
                var error = utError.get(err.message && err.message.split('\n').shift()) || errors.sql;
                reject(error(err));
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
                } else if (message.process === 'xls') { // todo
                    reject(errors.notImplemented(message.process));
                } else if (message.process === 'xml') { // todo
                    reject(errors.notImplemented(message.process));
                } else if (message.process === 'csv') { // todo
                    reject(errors.notImplemented(message.process));
                } else if (message.process === 'processRows') { // todo
                    reject(errors.notImplemented(message.process));
                } else if (message.process === 'queueRows') { // todo
                    reject(errors.notImplemented(message.process));
                } else {
                    reject(errors.missingProcess(message.process));
                }
            }
        });
    });
};

SqlPort.prototype.getSchema = function() {
    var result = [];
    if (this.config.schema) {
        var schema;
        if (typeof (this.config.schema) === 'function') {
            schema = this.config.schema();
        } else {
            schema = this.config.schema;
        }
        if (Array.isArray(schema)) {
            result = schema.slice();
        } else {
            result.push({path: schema});
        }
    }
    this.config.imports && this.config.imports.forEach(function(imp) {
        imp.match(/\.schema$/) && Array.prototype.push.apply(result, this.config[imp]);
        if (this.includesConfig('updates', imp, true)) {
            this.config[imp + '.schema'] && Array.prototype.push.apply(result, this.config[imp + '.schema']);
        }
    }.bind(this));
    return result.reduce((all, schema) => {
        schema = (typeof schema === 'function') ? schema(this) : schema;
        schema && all.push(schema);
        return all;
    }, []);
};

function flatten(data) {
    var result = {};
    function recurse(cur, prop) {
        if (Object(cur) !== cur) {
            result[prop] = cur;
        } else if (Array.isArray(cur) || typeof cur === 'function') {
            result[prop] = cur;
        } else {
            var isEmpty = true;
            Object.keys(cur).forEach(function(p) {
                isEmpty = false;
                recurse(cur[p], prop ? prop + '.' + p : p);
            });
            if (isEmpty && prop) {
                result[prop] = {};
            }
        }
    }
    recurse(data, '');
    return result;
}

SqlPort.prototype.updateSchema = function(schema) {
    this.checkConnection();
    var busConfig = flatten(this.bus.config);

    function replaceAuditLog(statement) {
        var parserSP = require('./parsers/mssqlSP');
        var binding = parserSP.parse(statement);
        return statement.trim().replace(AUDIT_LOG, mssqlQueries.auditLog(binding));
    }

    function replaceCallParams(statement) {
        var parserSP = require('./parsers/mssqlSP');
        var binding = parserSP.parse(statement);
        return statement.trim().replace(CALL_PARAMS, mssqlQueries.callParams(binding));
    }

    function replaceCoreError(statement, fileName, objectName, params) {
        return statement
            .split('\n')
            .map((line, index) => (line.replace(CORE_ERROR,
                `DECLARE @CORE_ERROR_FILE_${index} sysname='${fileName.replace(/'/g, '\'\'')}' ` +
                `DECLARE @CORE_ERROR_LINE_${index} int='${index + 1}' ` +
                `EXEC [core].[errorStack] @procid=@@PROCID, @file=@CORE_ERROR_FILE_${index}, @fileLine=@CORE_ERROR_LINE_${index}, @params = ${params}`)))
            .join('\n');
    }

    function preProcess(statement, fileName, objectName) {
        statement = statement.replace(VAR, (placeHolder, label) => busConfig[label] || placeHolder);

        if (statement.match(AUDIT_LOG)) {
            statement = replaceAuditLog(statement);
        }
        var params = 'NULL';
        if (statement.match(CALL_PARAMS)) {
            statement = replaceCallParams(statement);
            params = '@callParams';
        }
        if (statement.match(CORE_ERROR)) {
            statement = replaceCoreError(statement, fileName, objectName, params);
        }
        return statement;
    }

    function getAlterStatement(statement, fileName, objectName) {
        statement = preProcess(statement, fileName, objectName);
        if (statement.trim().match(/^CREATE\s+TYPE/i)) {
            return statement.trim();
        } else {
            return statement.trim().replace(/^CREATE /i, 'ALTER ');
        }
    }

    function tableToType(statement) {
        if (statement.match(/^CREATE\s+TABLE/i)) {
            var parserSP = require('./parsers/mssqlSP');
            var binding = parserSP.parse(statement);
            if (binding.type === 'table') {
                var name = binding.name.match(/]$/) ? binding.name.slice(0, -1) + 'TT]' : binding.name + 'TT';
                var columns = binding.fields.map(function(field) {
                    changeRowVersionType(field);
                    return `[${field.column}] [${field.type}]` +
                        (field.length !== null && field.scale !== null ? `(${field.length},${field.scale})` : '') +
                        (field.length !== null && field.scale === null ? `(${field.length})` : '') +
                        (typeof field.default === 'number' ? ` DEFAULT(${field.default})` : '') +
                        (typeof field.default === 'string' ? ` DEFAULT('${field.default.replace(/'/g, '\'\'')}')` : '');
                });
                return 'CREATE TYPE ' + name + ' AS TABLE (\r\n  ' + columns.join(',\r\n  ') + '\r\n)';
            }
        }
        return '';
    }

    function tableToTTU(statement) {
        var result = '';
        if (statement.match(/^CREATE\s+TABLE/i)) {
            var parserSP = require('./parsers/mssqlSP');
            var binding = parserSP.parse(statement);
            if (binding.type === 'table') {
                var name = binding.name.match(/]$/) ? binding.name.slice(0, -1) + 'TTU]' : binding.name + 'TTU';
                var columns = binding.fields.map(function(field) {
                    changeRowVersionType(field);
                    return ('[' + field.column + '] [' + field.type + ']' +
                        (field.length !== null && field.scale !== null ? `(${field.length},${field.scale})` : '') +
                        (field.length !== null && field.scale === null ? `(${field.length})` : '') +
                        ',\r\n' + field.column + 'Updated bit');
                });
                result = 'CREATE TYPE ' + name + ' AS TABLE (\r\n  ' + columns.join(',\r\n  ') + '\r\n)';
            }
        }
        return result;
    }

    function getCreateStatement(statement, fileName, objectName) {
        return preProcess(statement, fileName, objectName).trim()
            .replace(/^ALTER /i, 'CREATE ')
            .replace(/^DROP SYNONYM .* CREATE SYNONYM/i, 'CREATE SYNONYM');
    }

    function getSource(statement, fileName, objectName) {
        statement = preProcess(statement, fileName, objectName);
        if (statement.trim().match(/^CREATE\s+TYPE/i)) {
            var parserSP = require('./parsers/mssqlSP');
            var binding = parserSP.parse(statement);
            if (binding && binding.type === 'table type') {
                return binding.fields.map(fieldSource).join('\r\n');
            }
        }
        return statement.trim().replace(/^ALTER /i, 'CREATE ');
    }

    function addQuery(queries, params) {
        if (schema.source[params.objectId] === undefined) {
            queries.push({fileName: params.fileName, objectName: params.objectName, objectId: params.objectId, content: params.createStatement});
        } else {
            if (schema.source[params.objectId].length &&
                (getSource(params.fileContent, params.fileName, params.objectName) !== schema.source[params.objectId])) {
                var deps = schema.deps[params.objectId];
                if (deps) {
                    deps.names.forEach(function(dep) {
                        delete schema.source[dep];
                    });
                    queries.push({
                        fileName: params.fileName,
                        objectName: params.objectName + ' drop dependencies',
                        objectId: params.objectId,
                        content: deps.drop.join('\r\n')
                    });
                }
                queries.push({
                    fileName: params.fileName,
                    objectName: params.objectName,
                    objectId: params.objectId,
                    content: getAlterStatement(params.fileContent, params.fileName, params.objectName)
                });
            }
        }
    }

    function getObjectName(fileName) {
        return fileName.replace(/\.sql$/i, '').replace(/^[^$]*\$/, ''); // remove "prefix$" and ".sql" suffix
    }

    function shouldCreateTT(tableName) {
        return (self.config.createTT === true || self.includesConfig('tableToType', tableName, false)) && !self.includesConfig('skipTableType', tableName, false);
    }

    function retryFailedQueries(failedQueue) {
        var newFailedQueue = [];
        var request = self.getRequest();
        var errCollection = [];
        self.log.warn && self.log.warn('Retrying failed TX');
        return when.map(failedQueue, (schema) => {
            return request
                .batch(schema.content)
                .then((r) => {
                    self.log.warn && self.log.warn({message: schema.objectName, $meta: {opcode: 'updateFailedSchemas'}});
                    return true;
                })
                .catch((err) => {
                    var newErr = err;
                    newErr.fileName = schema.fileName;
                    newErr.message = newErr.message + ' (' + newErr.fileName + ':' + (newErr.lineNumber || 1) + ':1)';
                    self.log.error && self.log.error(newErr);
                    errCollection.push(newErr);
                    newFailedQueue.push(schema);
                });
        })
        .then((res) => {
            if (newFailedQueue.length === 0) {
                return;
            } else if (newFailedQueue.length === failedQueue.length) {
                throw errors.retryFailedSchemas(errCollection);
            }
            return retryFailedQueries(newFailedQueue);
        });
    }

    var self = this;
    var schemas = this.getSchema();
    var failedQueries = [];
    if (!schemas) {
        return schema;
    }
    return when.reduce(schemas, function(prev, schemaConfig) { // visit each schema folder
        return when.promise(function(resolve, reject) {
            fs.readdir(schemaConfig.path, function(err, files) {
                if (err) {
                    reject(err);
                } else {
                    var queries = [];
                    files = files.sort();
                    var objectIds = files.reduce(function(prev, cur) {
                        prev[getObjectName(cur).toLowerCase()] = true;
                        return prev;
                    }, {});
                    files.forEach(function(file) {
                        var objectName = getObjectName(file);
                        var objectId = objectName.toLowerCase();
                        var fileName = schemaConfig.path + '/' + file;
                        if (!fs.statSync(fileName).isFile()) {
                            return;
                        }
                        schemaConfig.linkSP && (prev[objectId] = fileName);
                        var fileContent = fs.readFileSync(fileName).toString();
                        addQuery(queries, {
                            fileName: fileName,
                            objectName: objectName,
                            objectId: objectId,
                            fileContent: fileContent,
                            createStatement: getCreateStatement(fileContent, fileName, objectName)
                        });
                        if (shouldCreateTT(objectId) && !objectIds[objectId + 'tt']) {
                            var tt = tableToType(fileContent.trim().replace(/^ALTER /i, 'CREATE '));
                            if (tt) {
                                addQuery(queries, {
                                    fileName: fileName,
                                    objectName: objectName + 'TT',
                                    objectId: objectId + 'tt',
                                    fileContent: tt,
                                    createStatement: tt
                                });
                            }
                            var ttu = tableToTTU(fileContent.trim().replace(/^ALTER /i, 'CREATE '));
                            if (ttu) {
                                addQuery(queries, {
                                    fileName: fileName,
                                    objectName: objectName + 'TTU',
                                    objectId: objectId + 'ttu',
                                    fileContent: ttu,
                                    createStatement: ttu
                                });
                            }
                        }
                    });

                    var request = self.getRequest();
                    var updated = [];
                    return when.reduce(queries, function(result, query) {
                        return request
                            .batch(query.content)
                            .then(() => {
                                updated.push(query.objectName);
                                return true;
                            })
                            .catch((e) => {
                                failedQueries.push(query);
                                if (self.log.warn) {
                                    self.log.warn({'failing file': query.fileName});
                                    self.log.warn(e);
                                }
                                return false;
                            });
                    }, [])
                    .then(function() {
                        updated.length && self.log.info && self.log.info({message: updated, $meta: {mtid: 'event', opcode: 'updateSchema'}});
                        resolve(prev);
                        return true;
                    });
                }
            });
        }, []);
    }, [])
    .then((objectList) => {
        if (!failedQueries.length) {
            return objectList;
        }
        return retryFailedQueries(failedQueries)
            .then(() => (objectList));
    })
    .then(function(objectList) {
        return self.loadSchema(objectList);
    });
};

SqlPort.prototype.execTemplate = function(template, params) {
    var self = this;
    return template.render(params).then(function(query) {
        return self.exec({query: query, process: 'json'})
            .then(result => result && result.dataSet);
    });
};

SqlPort.prototype.execTemplateRow = function(template, params) {
    return this.execTemplate(template, params).then(function(data) {
        var result = (data && data[0]) || {};
        if (result._errorCode && parseInt(result._errorCode, 10) !== 0) {
            // throw error if _errorCode is '', undefined, null, number (different than 0) or string (different than '0', '00', etc.)
            var error = errors.sql({
                code: result._errorCode || -1,
                message: result._errorMessage || 'sql error'
            });
            throw error;
        } else {
            return result;
        }
    });
};

SqlPort.prototype.execTemplateRows = function(template, params) {
    return this.execTemplate(template, params).then(function(data) {
        var result = data || [{}];
        if (result[0] && result[0]._errorCode && parseInt(result[0]._errorCode, 10) !== 0) {
            // throw error if _errorCode is '', undefined, null, number (different than 0) or string (different than '0', '00', etc.)
            var error = errors.sql({
                code: result[0]._errorCode || -1,
                message: result[0]._errorMessage || 'sql error'
            });
            throw error;
        } else {
            return result;
        }
    });
};

SqlPort.prototype.getRequest = function() {
    var request = new mssql.Request(this.connection);
    request.on('info', (info) => {
        this.log.warn && this.log.warn({$meta: {mtid: 'event', opcode: 'message'}, message: info});
    });
    return request;
};

SqlPort.prototype.callSP = function(name, params, flatten, fileName) {
    var self = this;
    var outParams = [];

    params && params.forEach(function(param) {
        param.out && outParams.push(param.name);
    });

    function sqlType(def) {
        var type;
        if (def.type === 'table') {
            type = def.create();
        } else if (def.type === 'rowversion') {
            type = mssql[ROW_VERSION_INNER_TYPE](8);
        } else {
            type = mssql[def.type.toUpperCase()];
        }
        if (def.size) {
            if (Array.isArray(def.size)) {
                type = type(def.size[0], def.size[1]);
            } else {
                type = (def.size === 'max') ? type(mssql.MAX) : type(def.size);
            }
        }
        return type;
    }

    function flattenMessage(data, delimiter) {
        if (!delimiter) {
            return data;
        }
        var result = {};
        function recurse(cur, prop) {
            if (Object(cur) !== cur) {
                result[prop] = cur;
            } else if (Array.isArray(cur)) {
                // for (var i = 0, l = cur.length; i < l; i += 1) {
                //     recurse(cur[i], prop + '[' + i + ']');
                // }
                // if (l === 0) {
                //     result[prop] = [];
                // }
                result[prop] = cur;
            } else {
                var isEmpty = true;
                for (var p in cur) {
                    isEmpty = false;
                    recurse(cur[p], prop ? prop + delimiter + p : p);
                }
                if (isEmpty && prop) {
                    result[prop] = {};
                }
            }
        }
        recurse(data, '');
        return result;
    }
    function getValue(column, value, def, updated) {
        if (updated) {
            return updated;
        }
        if (value === undefined) {
            return def;
        } else if (value) {
            if (/^(date.*|smalldate.*)$/.test(column.type.declaration)) {
                // set a javascript date for 'date', 'datetime', 'datetime2' 'smalldatetime' and 'time'
                return new Date(value);
            } else if (column.type.declaration === 'time') {
                return new Date('1970-01-01T' + value);
            } else if (column.type.declaration === 'xml') {
                var obj = {};
                obj[column.name] = value;
                return xmlBuilder.buildObject(obj);
            } else if (value.type === 'Buffer') {
                return Buffer.from(value.data);
            }
        }
        return value;
    }
    return function callLinkedSP(msg, $meta) {
        self.checkConnection(true);
        var request = self.getRequest();
        var data = flattenMessage(msg, flatten);
        var debug = this.isDebug();
        var debugParams = {};
        request.multiple = true;
        $meta.globalId = uuid.v1();
        params && params.forEach(function(param) {
            var value;
            if (param.name === 'meta') {
                value = $meta;
            } else if (param.update) {
                value = data[param.name] || data.hasOwnProperty(param.update);
            } else {
                value = data[param.name];
            }
            var hasValue = value !== void 0;
            var type = sqlType(param.def);
            debug && (debugParams[param.name] = value);
            if (param.def && param.def.type === 'time' && value != null) {
                value = new Date('1970-01-01T' + value);
            } else if (param.def && /datetime/.test(param.def.type) && value != null && !(value instanceof Date)) {
                value = new Date(value);
            } else if (param.def && param.def.type === 'xml' && value != null) {
                value = xmlBuilder.buildObject(value);
            } else if (param.def && param.def.type === 'rowversion' && value != null && !Buffer.isBuffer(value)) {
                value = Buffer.from(value.data ? value.data : []);
            }
            if (param.out) {
                request.output(param.name, type, value);
            } else {
                if (param.def && param.def.type === 'table') {
                    if (value) {
                        if (Array.isArray(value)) {
                            value.forEach(function(row) {
                                row = flattenMessage(row, param.flatten);
                                if (typeof row === 'object') {
                                    type.rows.add.apply(type.rows, param.columns.reduce(function(prev, cur, i) {
                                        prev.push(getValue(type.columns[i], row[cur.column], cur.default, cur.update && row.hasOwnProperty(cur.update)));
                                        return prev;
                                    }, []));
                                } else {
                                    type.rows.add.apply(type.rows, [getValue(type.columns[0], row, param.columns[0].default, false)]
                                        .concat(new Array(param.columns.length - 1)));
                                }
                            });
                        } else if (typeof value === 'object') {
                            value = flattenMessage(value, param.flatten);
                            type.rows.add.apply(type.rows, param.columns.reduce(function(prev, cur, i) {
                                prev.push(getValue(type.columns[i], value[cur.column], cur.default, cur.update && value.hasOwnProperty(cur.update)));
                                return prev;
                            }, []));
                        } else {
                            value = flattenMessage(value, param.flatten);
                            type.rows.add.apply(type.rows, [getValue(type.columns[0], value, param.columns[0].default, false)]
                                .concat(new Array(param.columns.length - 1)));
                        }
                    }
                    request.input(param.name, type);
                } else {
                    if (!param.default || hasValue) {
                        request.input(param.name, type, value);
                    }
                }
            }
        });
        return request.execute(name)
            .then(function(resultSets) {
                return when.map(resultSets, function(resultset) {
                    var xmlColumns = Object.keys(resultset.columns).reduce(function(columns, column) {
                        if (resultset.columns[column].type.declaration === 'xml') {
                            columns.push(column);
                        }
                        return columns;
                    }, []);
                    if (xmlColumns.length) {
                        return when.map(resultset, function(record) {
                            return when.map(xmlColumns, function(key) {
                                if (record[key]) { // value is not null
                                    return when.promise(function(resolve, reject) {
                                        xmlParser.parseString(record[key], function(err, result) {
                                            if (err) {
                                                throw errors.wrongXmlFormat({
                                                    xml: record[key]
                                                });
                                            } else {
                                                record[key] = result;
                                                resolve();
                                            }
                                        });
                                    });
                                }
                            });
                        });
                    }
                })
                .then(function() {
                    return resultSets;
                });
            })
            .then(function(resultSets) {
                function isNamingResultSet(element) {
                    return Array.isArray(element) &&
                        element.length === 1 &&
                        element[0].hasOwnProperty('resultSetName') &&
                        typeof element[0].resultSetName === 'string';
                }
                var error;
                if (resultSets.length > 0 && isNamingResultSet(resultSets[0])) {
                    var namedSet = {};
                    if (outParams.length) {
                        namedSet[self.config.paramsOutName] = outParams.reduce(function(prev, curr) {
                            prev[curr] = request.parameters[curr].value;
                            return prev;
                        }, {});
                    }
                    var name = null;
                    var single = false;
                    for (var i = 0; i < resultSets.length; ++i) {
                        if (name == null) {
                            if (!isNamingResultSet(resultSets[i])) {
                                throw errors.invalidResultSetOrder({
                                    expectName: true
                                });
                            } else {
                                name = resultSets[i][0].resultSetName;
                                single = !!resultSets[i][0].single;
                                if (name === 'ut-error') {
                                    error = utError.get(resultSets[i][0] && resultSets[i][0].type) || errors.sql;
                                    error = Object.assign(error(), resultSets[i][0]);
                                    name = null;
                                    single = false;
                                }
                            }
                        } else {
                            if (isNamingResultSet(resultSets[i])) {
                                throw errors.invalidResultSetOrder({
                                    expectName: false
                                });
                            }
                            if (namedSet.hasOwnProperty(name)) {
                                throw errors.duplicateResultSetName({
                                    name: name
                                });
                            }
                            if (single) {
                                if (resultSets[i].length === 0) {
                                    namedSet[name] = null;
                                } else if (resultSets[i].length === 1) {
                                    namedSet[name] = resultSets[i][0];
                                } else {
                                    throw errors.singleResultExpected({
                                        count: resultSets[i].length
                                    });
                                }
                            } else {
                                namedSet[name] = resultSets[i];
                            }
                            name = null;
                            single = false;
                        }
                    }
                    if (name != null) {
                        throw errors.invalidResultSetOrder({
                            expectName: false
                        });
                    }
                    if (error) {
                        Object.assign(error, namedSet);
                        throw error;
                    } else {
                        return namedSet;
                    }
                }
                if (outParams.length) {
                    resultSets.push([outParams.reduce(function(prev, curr) {
                        prev[curr] = request.parameters[curr].value;
                        return prev;
                    }, {})]);
                }
                return resultSets;
            })
            .catch(function(err) {
                var errorLines = err.message && err.message.split('\n');
                err.message = errorLines.shift();
                var error = utError.get(err.message) || errors.sql;
                var errToThrow = error(err);
                if (debug) {
                    err.storedProcedure = name;
                    err.params = debugParams;
                    err.fileName = fileName + ':1:1';
                    var stack = errToThrow.stack.split('\n');
                    stack.splice.apply(stack, [1, 0].concat(errorLines));
                    errToThrow.stack = stack.join('\n');
                }
                throw errToThrow;
            });
    };
};

SqlPort.prototype.linkSP = function(schema) {
    if (schema.parseList.length) {
        var parserSP = require('./parsers/mssqlSP');
        schema.parseList.forEach(function(procedure) {
            var binding = parserSP.parse(procedure.source);
            var flatName = binding.name.replace(/[[\]]/g, '');
            if (binding && binding.type === 'procedure') {
                var update = [];
                var flatten = false;
                binding.params && binding.params.forEach(function(param) {
                    update.push(param.name + '$update');
                    // flatten in case a parameters name have at least one underscore character surrounded by non underscore characters
                    if (!flatten && param.name.match(/\./)) {
                        flatten = '.';
                    } else if (!flatten && param.name.match(/[^_]_[^_]/)) {
                        flatten = '_';
                    }
                });
                binding.params && binding.params.forEach(function(param) {
                    (update.indexOf(param.name) >= 0) && (param.update = param.name.replace(/\$update$/i, ''));
                    if (param.def && param.def.type === 'table') {
                        var columns = schema.types[param.def.typeName.toLowerCase()];
                        param.columns = [];
                        param.flatten = false;
                        columns.forEach(function(column) {
                            if (column.column.match(/Updated$/)) {
                                column.update = column.column.replace(/Updated$/, '');
                            }
                            param.columns.push(column);
                            if (column.column.match(/\./)) {
                                param.flatten = '.';
                            }
                        });
                        param.def.create = function() {
                            var table = new mssql.Table(param.def.typeName.toLowerCase());
                            columns && columns.forEach(function(column) {
                                changeRowVersionType(column);
                                var type = mssql[column.type.toUpperCase()];
                                if (!(type instanceof Function)) {
                                    throw errors.unexpectedType({
                                        type: column.type,
                                        procedure: binding.name
                                    });
                                }
                                if (typeof column.length === 'string' && column.length.match(/^max$/i)) {
                                    table.columns.add(column.column, type(mssql.MAX));
                                } else {
                                    table.columns.add(column.column, type(column.length !== null ? Number.parseInt(column.length) : column.length, column.scale));
                                }
                            });
                            return table;
                        };
                    }
                });
                this.super[flatName] = this.callSP(binding.name, binding.params, flatten, procedure.fileName).bind(this);
                if (!this.config[flatName]) {
                    this.config[flatName] = this.super[flatName];
                }
            }
        }.bind(this));
    }
    return schema;
};

SqlPort.prototype.loadSchema = function(objectList) {
    var self = this;
    var schema = this.getSchema();
    if (((Array.isArray(schema) && !schema.length) || !schema) && !this.config.linkSP) {
        return {source: {}, parseList: []};
    }

    this.checkConnection();
    var request = this.getRequest();
    request.multiple = true;

    return request.query(mssqlQueries.loadSchema()).then(function(result) {
        var schema = {source: {}, parseList: [], types: {}, deps: {}};
        result[0].reduce(function(prev, cur) { // extract source code of procedures, views, functions, triggers
            cur.namespace = cur.namespace && cur.namespace.toLowerCase();
            cur.full = cur.full && cur.full.toLowerCase();
            if (cur.source) {
                prev.source[cur.full] = (prev.source[cur.full] || '') + (cur.source || '');
            } else if (cur.full) {
                prev.source[cur.full] = '';
            } else {
                prev.source[cur.namespace] = '';
            }
            if ((cur.type === 'P') && (cur.colid === 1) && (self.config.linkSP || (objectList && objectList[cur.full]))) {
                if (self.includesConfig('linkSP', [cur.full, cur.namespace], true)) {
                    prev.parseList.push({
                        source: cur.source,
                        fileName: objectList && objectList[cur.full]
                    });
                }
            };
            return prev;
        }, schema);
        result[1].reduce(function(prev, cur) { // extract columns of user defined table types
            var parserDefault = require('./parsers/mssqlDefault');
            changeRowVersionType(cur);
            if (!(mssql[cur.type.toUpperCase()] instanceof Function)) {
                throw errors.unexpectedColumnType({
                    type: cur.type,
                    userDefinedTableType: cur.name
                });
            }
            cur.name = cur.name && cur.name.toLowerCase();
            try {
                cur.default && (cur.default = parserDefault.parse(cur.default));
            } catch (err) {
                err.type = cur.type;
                err.userDefinedTableType = cur.name;
                throw errors.parserError(err);
            }
            var type = prev[cur.name] || (prev[cur.name] = []);
            type.push(cur);
            return prev;
        }, schema.types);
        result[2].reduce(function(prev, cur) { // extract dependencies
            cur.name = cur.name && cur.name.toLowerCase();
            cur.type = cur.type && cur.type.toLowerCase();
            var dep = prev[cur.type] || (prev[cur.type] = {names: [], drop: []});
            if (dep.names.indexOf(cur.name) < 0) {
                dep.names.push(cur.name);
                dep.drop.push(cur.drop);
            }
            return prev;
        }, schema.deps);
        Object.keys(schema.types).forEach(function(type) { // extract pseudo source code of user defined table types
            schema.source[type] = schema.types[type].map(fieldSource).join('\r\n');
        });
        return schema;
    });
};

SqlPort.prototype.refreshView = function(drop, data) {
    this.checkConnection();
    var schema = this.getSchema();
    if ((Array.isArray(schema) && !schema.length) || !schema) {
        return data;
    }
    var request = this.getRequest();
    return request.query(mssqlQueries.refreshView(drop)).then(function(result) {
        if (!drop && result && result.length) {
            throw errors.invalidView(result);
        } else {
            return data;
        }
    });
};

SqlPort.prototype.doc = function(schema) {
    if (!this.config.doc) {
        return schema;
    }
    this.checkConnection();
    var self = this;
    var schemas = this.getSchema();
    var parserSP = require('./parsers/mssqlSP');
    return when.reduce(schemas, function(prev, schemaConfig) { // visit each schema folder
        return when.promise(function(resolve, reject) {
            fs.readdir(schemaConfig.path, function(err, files) {
                if (err) {
                    reject(err);
                } else {
                    files = files.sort();
                    files.forEach(function(file) {
                        var fileName = schemaConfig.path + '/' + file;
                        if (!fs.statSync(fileName).isFile()) {
                            return;
                        }
                        var fileContent = fs.readFileSync(fileName).toString();
                        if (fileContent.trim().match(/^(\bCREATE\b|\bALTER\b)\s+PROCEDURE/i) || fileContent.trim().match(/^(\bCREATE\b|\bALTER\b)\s+TABLE/i)) {
                            var binding = parserSP.parse(fileContent);
                            if (binding.type === 'procedure') {
                                binding.params.forEach((param) => {
                                    if (param.doc) {
                                        prev.push({
                                            type0: 'SCHEMA',
                                            name0: binding.schema,
                                            type1: 'PROCEDURE',
                                            name1: binding.table,
                                            type2: 'PARAMETER',
                                            name2: '@' + param.name,
                                            doc: param.doc
                                        });
                                    }
                                });
                            } else {
                                binding.fields.forEach((field) => {
                                    if (field.doc) {
                                        prev.push({
                                            type0: 'SCHEMA',
                                            name0: binding.schema,
                                            type1: 'TABLE',
                                            name1: binding.table,
                                            type2: 'COLUMN',
                                            name2: field.column,
                                            doc: field.doc
                                        });
                                    }
                                });
                            }
                        }
                    });
                    resolve(prev);
                }
            });
        }, []);
    }, [])
    .then(function(docList) {
        var request = self.getRequest();
        request.multiple = true;
        var docListParam = new mssql.Table('core.documentationTT');
        docListParam.columns.add('type0', mssql.VarChar(128));
        docListParam.columns.add('name0', mssql.NVarChar(128));
        docListParam.columns.add('type1', mssql.VarChar(128));
        docListParam.columns.add('name1', mssql.NVarChar(128));
        docListParam.columns.add('type2', mssql.VarChar(128));
        docListParam.columns.add('name2', mssql.NVarChar(128));
        docListParam.columns.add('doc', mssql.NVarChar(2000));
        docList.forEach(function(doc) {
            docListParam.rows.add(doc.type0, doc.name0, doc.type1, doc.name1, doc.type2, doc.name2, doc.doc);
        });
        request.input('docList', docListParam);
        return request.execute('core.documentation')
            .then(function() {
                return schema;
            });
    });
};

SqlPort.prototype.tryConnect = function() {
    this.connection = new mssql.Connection(this.config.db);
    if (this.config.create) {
        var conCreate = new mssql.Connection({
            server: this.config.db.server,
            user: this.config.create.user,
            password: this.config.create.password
        });
        return conCreate.connect()
        .then(() => (new mssql.Request(conCreate)).batch(mssqlQueries.createDatabase(this.config.db.database)))
        .then(() => this.config.create.diagram && new mssql.Request(conCreate).batch(mssqlQueries.enableDatabaseDiagrams(this.config.db.database)))
        .then(() => {
            if (this.config.create.user === this.config.db.user) {
                return;
            }
            return (new mssql.Request(conCreate)).batch(mssqlQueries.createUser(this.config.db.database, this.config.db.user, this.config.db.password));
        })
        .then(() => conCreate.close())
        .then(() => this.connection.connect())
        .catch((err) => {
            this.log && this.log.error && this.log.error({sourcePort: this.config.id, err});
            try { conCreate.close(); } catch (e) {};
            throw err;
        });
    } else {
        return this.connection.connect();
    }
};

module.exports = SqlPort;
