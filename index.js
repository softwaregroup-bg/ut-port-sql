'use strict';
const merge = require('lodash.merge');
const mssql = require('ut-mssql');
const util = require('util');
const fs = require('fs');
const crypto = require('./crypto');
const mssqlQueries = require('./sql');
const xml2js = require('xml2js');
const uuid = require('uuid');
const through2 = require('through2');
const path = require('path');
const xmlParser = new xml2js.Parser({explicitRoot: false, charkey: 'text', mergeAttrs: true, explicitArray: false});
const xmlBuilder = new xml2js.Builder({headless: true});
const AUDIT_LOG = /^[\s+]{0,}--ut-audit-params$/m;
const CORE_ERROR = /^[\s+]{0,}EXEC \[?core]?\.\[?error]?$/m;
const CALL_PARAMS = /^[\s+]{0,}DECLARE @callParams XML$/m;
const VAR_RE = /\$\{([^}]*)\}/g;
const ROW_VERSION_INNER_TYPE = 'BINARY';
let errors;

function changeRowVersionType(field) {
    if (field && (field.type.toUpperCase() === 'ROWVERSION' || field.type.toUpperCase() === 'TIMESTAMP')) {
        field.type = ROW_VERSION_INNER_TYPE;
        field.length = 8;
    }
}

module.exports = function({parent}) {
    function SqlPort({config}) {
        parent && parent.apply(this, arguments);
        this.config = merge({
            id: null,
            logLevel: 'info',
            retrySchemaUpdate: true,
            type: 'sql',
            createTT: false,
            retry: 10000,
            tableToType: {},
            skipTableType: [],
            paramsOutName: 'out',
            doc: false,
            db: {
                options: {
                    debug: {
                        packet: true
                    }
                }
            }
        }, config);
        errors = errors || require('./errors')(this.defineError);
        this.super = {};
        this.connection = null;
        this.retryTimeout = null;
        this.connectionAttempt = 0;
        return this;
    }

    function fieldSource(column) {
        return (column.column + '\t' + column.type + '\t' + (column.length === null ? '' : column.length) + '\t' + (column.scale === null ? '' : column.scale)).toLowerCase();
    }

    if (parent) {
        util.inherits(SqlPort, parent);
    }

    SqlPort.prototype.init = function init() {
        parent && parent.prototype.init.apply(this, arguments);
        this.latency = this.counter && this.counter('average', 'lt', 'Latency');
        this.bytesSent = this.counter && this.counter('counter', 'bs', 'Bytes sent', 300);
        this.bytesReceived = this.counter && this.counter('counter', 'br', 'Bytes received', 300);
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
        return Promise.resolve()
            .then(() => parent && parent.prototype.start.apply(this, Array.prototype.slice.call(arguments)))
            .then(this.connect.bind(this))
            .then(result => {
                this.pull(this.exec);
                return result;
            });
    };

    SqlPort.prototype.stop = function stop() {
        clearTimeout(this.retryTimeout);
        // this.queue.push();
        this.connectionReady = false;
        this.connection.close();
        parent && parent.prototype.stop.apply(this, Array.prototype.slice.call(arguments));
    };

    function setPathProperty(object, fieldName, fieldValue) {
        let path = fieldName.split('.');
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
        let $meta = (arguments.length && arguments[arguments.length - 1]);
        $meta.debug = !!this.bus.config.debug;
        let methodName = ($meta && $meta.method);
        if (methodName) {
            let parts = methodName.match(/^([^[]*)(\[[0+?^]?])?$/);
            let modifier;
            if (parts) {
                methodName = parts[1];
                modifier = parts[2];
            }
            let method = this.config[methodName];
            if (!method) {
                methodName = methodName.split('/', 2);
                method = methodName.length === 2 && this.config[methodName[1]];
            }
            if (method instanceof Function) {
                return Promise.resolve()
                    .then(() => method.apply(this, Array.prototype.slice.call(arguments)))
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

        // let start = +new Date();
        let debug = this.isDebug();
        let request = this.getRequest();
        let port = this;
        return new Promise(function(resolve, reject) {
            request.query(message.query, function(err, result) {
                // let end = +new Date();
                // let execTime = end - start;
                // todo record execution time
                if (err) {
                    debug && (err.query = message.query);
                    let error = port.getError(err.message && err.message.split('\n').shift()) || errors.sql;
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
        let result = [];
        if (this.config.schema) {
            let schema;
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
        let result = {};
        function recurse(cur, prop) {
            if (Object(cur) !== cur) {
                result[prop] = cur;
            } else if (Array.isArray(cur) || typeof cur === 'function') {
                result[prop] = cur;
            } else {
                let isEmpty = true;
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
        let busConfig = flatten(this.bus.config);

        function replaceAuditLog(statement) {
            let parserSP = require('./parsers/mssqlSP');
            let binding = parserSP.parse(statement);
            return statement.trim().replace(AUDIT_LOG, mssqlQueries.auditLog(binding));
        }

        function replaceCallParams(statement) {
            let parserSP = require('./parsers/mssqlSP');
            let binding = parserSP.parse(statement);
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
            statement = statement.replace(VAR_RE, (placeHolder, label) => busConfig[label] || placeHolder);

            if (statement.match(AUDIT_LOG)) {
                statement = replaceAuditLog(statement);
            }
            let params = 'NULL';
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
                let parserSP = require('./parsers/mssqlSP');
                let binding = parserSP.parse(statement);
                if (binding.type === 'table') {
                    let name = binding.name.match(/]$/) ? binding.name.slice(0, -1) + 'TT]' : binding.name + 'TT';
                    let columns = binding.fields.map(function(field) {
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
            let result = '';
            if (statement.match(/^CREATE\s+TABLE/i)) {
                let parserSP = require('./parsers/mssqlSP');
                let binding = parserSP.parse(statement);
                if (binding.type === 'table') {
                    let name = binding.name.match(/]$/) ? binding.name.slice(0, -1) + 'TTU]' : binding.name + 'TTU';
                    let columns = binding.fields.map(function(field) {
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
                let parserSP = require('./parsers/mssqlSP');
                let binding = parserSP.parse(statement);
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
                    let deps = schema.deps[params.objectId];
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

        function retrySchemaUpdate(failedQueue) {
            let newFailedQueue = [];
            let request = self.getRequest();
            let errCollection = [];
            self.log.warn && self.log.warn('Retrying failed TX');
            let promise = Promise.resolve();
            failedQueue.forEach(function(schema) {
                promise = promise
                    .then(function retryFailedQueueSchema() {
                        return request
                            .batch(schema.content)
                            .then((r) => {
                                self.log.warn && self.log.warn({
                                    message: schema.objectName,
                                    $meta: {
                                        opcode: 'updateFailedSchemas'
                                    }
                                });
                                return true;
                            })
                            .catch((err) => {
                                let newErr = err;
                                newErr.fileName = schema.fileName;
                                newErr.message = newErr.message + ' (' + newErr.fileName + ':' + (newErr.lineNumber || 1) + ':1)';
                                self.log.error && self.log.error(newErr);
                                errCollection.push(newErr);
                                newFailedQueue.push(schema);
                                return false;
                            });
                    });
            });
            return promise
                .then(() => {
                    if (newFailedQueue.length === 0) {
                        return;
                    } else if (newFailedQueue.length === failedQueue.length) {
                        throw errors.retryFailedSchemas(errCollection);
                    }
                    return retrySchemaUpdate(newFailedQueue);
                });
        }

        let self = this;
        let schemas = this.getSchema();
        let failedQueries = [];
        if (!schemas || !schemas.length) {
            return schema;
        }
        return new Promise((resolve, reject) => {
            let objectList = [];
            let promise = Promise.resolve();
            schemas.forEach((schemaConfig) => {
                promise = promise
                    .then(() => {
                        return new Promise((resolve, reject) => {
                            fs.readdir(schemaConfig.path, (err, files) => {
                                if (err) {
                                    reject(err);
                                }
                                let queries = [];
                                files = files.sort();
                                if (schemaConfig.exclude && schemaConfig.exclude.length > 0) {
                                    files = files.filter((file) => !(schemaConfig.exclude.indexOf(file) >= 0));
                                }
                                let objectIds = files.reduce(function(prev, cur) {
                                    prev[getObjectName(cur).toLowerCase()] = true;
                                    return prev;
                                }, {});
                                files.forEach(function(file) {
                                    let objectName = getObjectName(file);
                                    let objectId = objectName.toLowerCase();
                                    let fileName = schemaConfig.path + '/' + file;
                                    if (!fs.statSync(fileName).isFile()) {
                                        return;
                                    }
                                    schemaConfig.linkSP && (objectList[objectId] = fileName);
                                    let fileContent = fs.readFileSync(fileName).toString();
                                    addQuery(queries, {
                                        fileName: fileName,
                                        objectName: objectName,
                                        objectId: objectId,
                                        fileContent: fileContent,
                                        createStatement: getCreateStatement(fileContent, fileName, objectName)
                                    });
                                    if (shouldCreateTT(objectId) && !objectIds[objectId + 'tt']) {
                                        let tt = tableToType(fileContent.trim().replace(/^ALTER /i, 'CREATE '));
                                        if (tt) {
                                            addQuery(queries, {
                                                fileName: fileName,
                                                objectName: objectName + 'TT',
                                                objectId: objectId + 'tt',
                                                fileContent: tt,
                                                createStatement: tt
                                            });
                                        }
                                        let ttu = tableToTTU(fileContent.trim().replace(/^ALTER /i, 'CREATE '));
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

                                let request = self.getRequest();
                                let updated = [];
                                let innerPromise = Promise.resolve();
                                queries.forEach((query) => {
                                    innerPromise = innerPromise.then(() => {
                                        return request
                                            .batch(query.content)
                                            .then(() => updated.push(query.objectName))
                                            .catch((err) => {
                                                if (!this.config.retrySchemaUpdate) {
                                                    let newErr = err;
                                                    newErr.fileName = query.fileName;
                                                    newErr.message = newErr.message + ' (' + newErr.fileName + ':' + (newErr.lineNumber || 1) + ':1)';
                                                    newErr.stack = newErr.stack.split('\n').shift();
                                                    throw newErr;
                                                } else {
                                                    failedQueries.push(query);
                                                    if (self.log.warn) {
                                                        self.log.warn({'failing file': query.fileName});
                                                        self.log.warn(err);
                                                    }
                                                    return false;
                                                }
                                            });
                                    });
                                });
                                return innerPromise
                                    .then(function() {
                                        updated.length && self.log.info && self.log.info({
                                            message: updated,
                                            $meta: {
                                                mtid: 'event',
                                                opcode: 'updateSchema'
                                            }
                                        });
                                        return resolve();
                                    })
                                    .catch(reject);
                            });
                        });
                    });
            });
            return promise
                .then(() => resolve(objectList))
                .catch(reject);
        })
        .then((objectList) => {
            if (!failedQueries.length) {
                return objectList;
            }
            return retrySchemaUpdate(failedQueries)
                .then(() => (objectList));
        })
        .then(function(objectList) {
            return self.loadSchema(objectList);
        });
    };

    SqlPort.prototype.execTemplate = function(template, params) {
        let self = this;
        return template.render(params).then(function(query) {
            return self.exec({query: query, process: 'json'})
                .then(result => result && result.dataSet);
        });
    };

    SqlPort.prototype.execTemplateRow = function(template, params) {
        return this.execTemplate(template, params).then(function(data) {
            let result = (data && data[0]) || {};
            if (result._errorCode && parseInt(result._errorCode, 10) !== 0) {
                // throw error if _errorCode is '', undefined, null, number (different than 0) or string (different than '0', '00', etc.)
                let error = errors.sql({
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
            let result = data || [{}];
            if (result[0] && result[0]._errorCode && parseInt(result[0]._errorCode, 10) !== 0) {
                // throw error if _errorCode is '', undefined, null, number (different than 0) or string (different than '0', '00', etc.)
                let error = errors.sql({
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
        let request = new mssql.Request(this.connection);
        request.on('info', (info) => {
            this.log.warn && this.log.warn({$meta: {mtid: 'event', opcode: 'message'}, message: info});
        });
        return request;
    };

    SqlPort.prototype.callSP = function(name, params, flatten, fileName) {
        let self = this;
        let outParams = [];

        params && params.forEach(function(param) {
            param.out && outParams.push(param.name);
        });

        function sqlType(def) {
            let type;
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
            let result = {};
            function recurse(cur, prop) {
                if (Object(cur) !== cur) {
                    result[prop] = cur;
                } else if (Array.isArray(cur)) {
                    // for (let i = 0, l = cur.length; i < l; i += 1) {
                    //     recurse(cur[i], prop + '[' + i + ']');
                    // }
                    // if (l === 0) {
                    //     result[prop] = [];
                    // }
                    result[prop] = cur;
                } else {
                    let isEmpty = true;
                    for (let p in cur) {
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
                    let obj = {};
                    obj[column.name] = value;
                    return xmlBuilder.buildObject(obj);
                } else if (value.type === 'Buffer') {
                    return Buffer.from(value.data);
                }
            }
            return value;
        }
        function transform(request) {
            let single;
            let namedSet;
            let comma = '';
            let counter = 1;
            request.on('recordset', function(cols) {
                counter++;
            });
            function getResultSetName(chunk) {
                let keys = Object.keys(chunk);
                return keys.length > 0 && keys[0].toLowerCase() === 'resultsetname' ? chunk[keys[0]] : null;
            }
            return request.pipe(through2({objectMode: true}, function(chunk, encoding, next) {
                if (namedSet === undefined) { // called only once to write object start literal
                    namedSet = !!getResultSetName(chunk);
                    this.push(namedSet ? '{' : '[');
                }
                if (counter % 2) { // handle rows
                    this.push(comma + JSON.stringify(chunk));
                    if (comma === '') {
                        comma = ',';
                    }
                } else { // handle resultsets
                    if (getResultSetName(chunk)) { // handle recordsets
                        if (single !== undefined) {
                            if (comma !== '') {
                                this.push(single ? '{},' : '],'); // handling empty result set
                            } else {
                                this.push(single ? '},' : '],'); // handling end
                            }
                        }
                        single = !!chunk.single;
                        this.push('"' + getResultSetName(chunk) + '":');
                        if (!single) {
                            this.push('['); // open an array
                        }
                        comma = '';
                    }
                }
                next();
            }, function(next) {
                if (namedSet !== undefined) {
                    if (single === false) {
                        this.push(']'); // push end of array literal If the last object is not a single
                    }
                    this.push(namedSet ? '}' : ']'); //  write object end literal
                }
                next();
            }));
        }
        return function callLinkedSP(msg, $meta) {
            self.checkConnection(true);
            let request = self.getRequest();
            let data = flattenMessage(msg, flatten);
            let debug = this.isDebug();
            let debugParams = {};
            request.multiple = true;
            $meta.globalId = uuid.v1();
            params && params.forEach(function(param) {
                let value;
                if (param.name === 'meta') {
                    value = $meta;
                } else if (param.update) {
                    value = data[param.name] || data.hasOwnProperty(param.update);
                } else {
                    value = data[param.name];
                }
                let hasValue = value !== void 0;
                let type = sqlType(param.def);
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
            if ($meta.saveAs) {
                let fileDir = path.dirname($meta.saveAs);
                return new Promise((resolve, reject) => {
                    fs.mkdir(fileDir, (e) => {
                        if (!e || e.code === 'EEXIST') {
                            return resolve();
                        }
                        return reject(e);
                    });
                }).then(function(resolve, reject) {
                    request.stream = true;
                    let ws = fs.createWriteStream($meta.saveAs);
                    transform(request).pipe(ws);
                    request.execute(name);
                    return new Promise(function(resolve, reject) {
                        ws.on('finish', function() {
                            return resolve({fileName: $meta.saveAs});
                        });
                        ws.on('error', function(err) {
                            return reject(err);
                        });
                    });
                });
            }
            return request.execute(name)
                .then(function(resultSets) {
                    let promise = Promise.resolve();
                    resultSets.forEach(function(resultset) {
                        let xmlColumns = Object.keys(resultset.columns)
                            .reduce(function(columns, column) {
                                if (resultset.columns[column].type.declaration === 'xml') {
                                    columns.push(column);
                                }
                                return columns;
                            }, []);
                        if (xmlColumns.length) {
                            resultset.forEach(function(record) {
                                xmlColumns.forEach(function(key) {
                                    if (record[key]) { // value is not null
                                        promise = promise
                                            .then(function() {
                                                return new Promise(function(resolve, reject) {
                                                    xmlParser.parseString(record[key], function(err, result) {
                                                        if (err) {
                                                            reject(errors.wrongXmlFormat({
                                                                xml: record[key]
                                                            }));
                                                        } else {
                                                            record[key] = result;
                                                            resolve();
                                                        }
                                                    });
                                                });
                                            });
                                    }
                                });
                            });
                        }
                    });
                    return promise.then(() => resultSets);
                })
                .then(function(resultSets) {
                    function isNamingResultSet(element) {
                        return Array.isArray(element) &&
                            element.length === 1 &&
                            element[0].hasOwnProperty('resultSetName') &&
                            typeof element[0].resultSetName === 'string';
                    }
                    let error;
                    if (resultSets.length > 0 && isNamingResultSet(resultSets[0])) {
                        let namedSet = {};
                        if (outParams.length) {
                            namedSet[self.config.paramsOutName] = outParams.reduce(function(prev, curr) {
                                prev[curr] = request.parameters[curr].value;
                                return prev;
                            }, {});
                        }
                        let name = null;
                        let single = false;
                        for (let i = 0; i < resultSets.length; ++i) {
                            if (name == null) {
                                if (!isNamingResultSet(resultSets[i])) {
                                    throw errors.invalidResultSetOrder({
                                        expectName: true
                                    });
                                } else {
                                    name = resultSets[i][0].resultSetName;
                                    single = !!resultSets[i][0].single;
                                    if (name === 'ut-error') {
                                        error = self.getError(resultSets[i][0] && resultSets[i][0].type) || errors.sql;
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
                    let errorLines = err.message && err.message.split('\n');
                    err.message = errorLines.shift();
                    let error = self.getError(err.type || err.message) || errors.sql;
                    let errToThrow = error(err);
                    if (debug) {
                        err.storedProcedure = name;
                        err.params = debugParams;
                        err.fileName = fileName + ':1:1';
                        let stack = errToThrow.stack.split('\n');
                        stack.splice.apply(stack, [1, 0].concat(errorLines));
                        errToThrow.stack = stack.join('\n');
                    }
                    throw errToThrow;
                });
        };
    };

    SqlPort.prototype.linkSP = function(schema) {
        if (schema.parseList.length) {
            let parserSP = require('./parsers/mssqlSP');
            schema.parseList.forEach(function(procedure) {
                let binding = parserSP.parse(procedure.source);
                let flatName = binding.name.replace(/[[\]]/g, '');
                if (binding && binding.type === 'procedure') {
                    let update = [];
                    let flatten = false;
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
                            let columns = schema.types[param.def.typeName.toLowerCase()];
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
                                let table = new mssql.Table(param.def.typeName.toLowerCase());
                                columns && columns.forEach(function(column) {
                                    changeRowVersionType(column);
                                    let type = mssql[column.type.toUpperCase()];
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
        let self = this;
        let schema = this.getSchema();
        if (((Array.isArray(schema) && !schema.length) || !schema) && !this.config.linkSP) {
            return {source: {}, parseList: []};
        }

        this.checkConnection();
        let request = this.getRequest();
        request.multiple = true;
        return request.query(mssqlQueries.loadSchema(this.config.updates === false)).then(function(result) {
            let schema = {source: {}, parseList: [], types: {}, deps: {}};
            result[0].reduce(function(prev, cur) { // extract source code of procedures, views, functions, triggers
                let full = cur.full;
                let namespace = cur.namespace;
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
                    if (self.includesConfig('linkSP', [full, namespace], true)) {
                        prev.parseList.push({
                            source: cur.source,
                            fileName: objectList && objectList[cur.full]
                        });
                    }
                };
                return prev;
            }, schema);
            result[1].reduce(function(prev, cur) { // extract columns of user defined table types
                let parserDefault = require('./parsers/mssqlDefault');
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
                let type = prev[cur.name] || (prev[cur.name] = []);
                type.push(cur);
                return prev;
            }, schema.types);
            result[2].reduce(function(prev, cur) { // extract dependencies
                cur.name = cur.name && cur.name.toLowerCase();
                cur.type = cur.type && cur.type.toLowerCase();
                let dep = prev[cur.type] || (prev[cur.type] = {names: [], drop: []});
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
        let schema = this.getSchema();
        if ((Array.isArray(schema) && !schema.length) || !schema) {
            return data;
        }
        return this.getRequest()
            .query(mssqlQueries.refreshView(drop))
            .then(function(result) {
                if (!drop && result && result.length) {
                    throw errors.invalidView(result);
                }
                return data;
            });
    };

    SqlPort.prototype.doc = function(schema) {
        if (!this.config.doc) {
            return schema;
        }
        this.checkConnection();
        let self = this;
        let schemas = this.getSchema();
        let parserSP = require('./parsers/mssqlSP');
        return new Promise(function(resolve, reject) {
            let docList = [];
            let promise = Promise.resolve();
            schemas.forEach(function(schemaConfig) {
                promise = promise
                    .then(function() {
                        return new Promise(function(resolve, reject) {
                            fs.readdir(schemaConfig.path, function(err, files) {
                                if (err) {
                                    return reject(err);
                                }
                                files = files.sort();
                                files.forEach(function(file) {
                                    let fileName = schemaConfig.path + '/' + file;
                                    if (!fs.statSync(fileName).isFile()) {
                                        return;
                                    }
                                    let fileContent = fs.readFileSync(fileName).toString();
                                    if (fileContent.trim().match(/^(\bCREATE\b|\bALTER\b)\s+PROCEDURE/i) || fileContent.trim().match(/^(\bCREATE\b|\bALTER\b)\s+TABLE/i)) {
                                        let binding = parserSP.parse(fileContent);
                                        if (binding.type === 'procedure') {
                                            binding.params.forEach((param) => {
                                                if (param.doc) {
                                                    docList.push({
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
                                                    docList.push({
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
                                resolve();
                            });
                        });
                    });
            });
            return promise
                .then(() => resolve(docList))
                .catch(reject);
        })
        .then(function(docList) {
            let request = self.getRequest();
            request.multiple = true;
            let docListParam = new mssql.Table('core.documentationTT');
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
        if (this.config.db) {
            this.config.db.beforeConnect = c => {
                if (c.debug) {
                    let id = (++this.connectionAttempt);
                    let created = new Date();
                    let context = {id, created};
                    let notify = (event, connection) => {
                        this.log.info && this.log.info({$meta: {mtid: 'event', opcode: 'port.connection.' + event}, connection});
                    };
                    c.debug.packet = (direction, packet) => {
                        if (direction === 'Sent') {
                            let length = packet.length();
                            this.bytesSent && this.bytesSent(length + 8);
                            if (this.log.trace) {
                                let id = packet.packetId();
                                if (id === 255 || packet.isLast()) {
                                    this.log.trace({
                                        $meta: {mtid: 'event', opcode: 'port.out'},
                                        size: length + id * c.messageIo.packetSize(),
                                        header: packet.headerToString()
                                    });
                                }
                            }
                        }
                        if (direction === 'Received') {
                            let length = packet.length();
                            this.bytesReceived && this.bytesReceived(length + 8);
                            if (this.log.trace) {
                                let id = packet.packetId();
                                if (id === 255 || packet.isLast()) {
                                    this.log.trace({
                                        $meta: {mtid: 'event', opcode: 'port.in'},
                                        size: length + id * c.messageIo.packetSize(),
                                        header: packet.headerToString()
                                    });
                                }
                            }
                        }
                    };
                    c.debug.log = msg => {
                        if (this.log.debug && c.state && (c.state.name !== 'LoggedIn') && (c.state.name !== 'SentClientRequest')) {
                            this.log.debug({$meta: {mtid: 'event', opcode: 'port.connection'}, message: msg, id, created});
                        }
                    };
                    c.once('connect', err => {
                        if (!err) {
                            let stream = c.messageIo.socket;
                            context = {
                                id,
                                created,
                                localAddress: stream.localAddress,
                                localPort: stream.localPort,
                                remoteAddress: stream.remoteAddress,
                                remotePort: stream.remotePort
                            };
                            notify('connected', context);
                        }
                    });
                    c.once('end', err => {
                        if (!err) {
                            notify('disconnected', context);
                        }
                    });
                }
            };
        };

        this.connection = new mssql.Connection(this.config.db);
        if (this.config.create) {
            let conCreate = new mssql.Connection({
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

    return SqlPort;
};
