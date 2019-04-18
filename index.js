'use strict';
const stringify = require('json-stringify-deterministic');
const mssql = require('mssql');
const fs = require('fs');
const fsplus = require('fs-plus');
const crypto = require('./crypto');
const mssqlQueries = require('./sql');
const xml2js = require('xml2js');
const uuid = require('uuid');
const path = require('path');
const xmlParser = new xml2js.Parser({explicitRoot: false, charkey: 'text', mergeAttrs: true, explicitArray: false});
const xmlBuilder = new xml2js.Builder({headless: true});
const saveAs = require('./saveAs');
const AUDIT_LOG = /^[\s+]{0,}--ut-audit-params$/m;
const CORE_ERROR = /^[\s+]{0,}EXEC \[?core]?\.\[?error]?$/m;
const CALL_PARAMS = /^[\s+]{0,}DECLARE @callParams XML$/m;
const VAR_RE = /\$\{([^}]*)\}/g;
const ENCRYPT_RE = /(?:NULL|0x.*)\/\*encrypt (.*)\*\//gi;
const ROW_VERSION_INNER_TYPE = 'BINARY';
const serverRequire = require;
const dotprop = require('dot-prop');
const isEncrypted = item => item && ((item.def && item.def.type === 'varbinary' && item.def.size % 16 === 0) || (item.length % 16 === 0) || /^encrypted/.test(item.name));

// patch for https://github.com/tediousjs/tedious/pull/710
require('tedious').TYPES.Time.writeParameterData = function writeParameterData(buffer, parameter, options) {
    if (parameter.value != null) {
        var time = new Date(+parameter.value);

        var timestamp = void 0;
        if (options.useUTC) {
            timestamp = ((time.getUTCHours() * 60 + time.getUTCMinutes()) * 60 + time.getUTCSeconds()) * 1000 + time.getUTCMilliseconds();
        } else {
            timestamp = ((time.getHours() * 60 + time.getMinutes()) * 60 + time.getSeconds()) * 1000 + time.getMilliseconds();
        }

        timestamp = timestamp * Math.pow(10, parameter.scale - 3);
        timestamp += (parameter.value.nanosecondDelta != null ? parameter.value.nanosecondDelta : 0) * Math.pow(10, parameter.scale);
        timestamp = Math.round(timestamp);

        switch (parameter.scale) {
            case 0:
            case 1:
            case 2:
                buffer.writeUInt8(3);
                buffer.writeUInt24LE(timestamp);
                break;
            case 3:
            case 4:
                buffer.writeUInt8(4);
                buffer.writeUInt32LE(timestamp);
                break;
            case 5:
            case 6:
            case 7:
                buffer.writeUInt8(5);
                buffer.writeUInt40LE(timestamp);
        }
    } else {
        buffer.writeUInt8(0);
    }
};
// end patch

function changeRowVersionType(field) {
    if (field && (field.type.toUpperCase() === 'ROWVERSION' || field.type.toUpperCase() === 'TIMESTAMP')) {
        field.type = ROW_VERSION_INNER_TYPE;
        field.length = 8;
    }
}

function interpolate(txt, params = {}) {
    return txt.replace(VAR_RE, (placeHolder, label) => {
        let value = dotprop.get(params, label);
        switch (typeof value) {
            case 'undefined': return placeHolder;
            case 'object': return JSON.stringify(value);
            default: return value;
        }
    });
};

module.exports = function({utPort}) {
    let sqlPortErrors;
    return class SqlPort extends utPort {
        constructor() {
            super(...arguments);
            if (!this.errors || !this.errors.getError) throw new Error('Please use the latest version of ut-port');
            sqlPortErrors = require('./errors')(this.errors);
            this.connection = null;
            this.retryTimeout = null;
            this.connectionAttempt = 0;
        }
        get defaults() {
            return {
                retrySchemaUpdate: true,
                type: 'sql',
                cache: false,
                offline: false,
                createTT: false,
                allowQuery: false,
                retry: 10000,
                tableToType: {},
                skipTableType: [],
                paramsOutName: 'out',
                doc: false,
                maxNesting: 5,
                connection: {
                    options: {
                        debug: {
                            packet: true
                        },
                        encrypt: false,
                        enableArithAbort: true,
                        enableAnsiWarnings: true,
                        abortTransactionOnError: true
                    }
                }
            };
        }
        async init() {
            const result = await super.init(...arguments);
            this.latency = this.counter && this.counter('average', 'lt', 'Latency');
            this.bytesSent = this.counter && this.counter('counter', 'bs', 'Bytes sent', 300);
            this.bytesReceived = this.counter && this.counter('counter', 'br', 'Bytes received', 300);
            return result;
        }
        connect() {
            this.connection && this.connection.close();
            this.connectionReady = false;
            return Promise.resolve()
                .then(() => {
                    if (this.config.connection.cryptoAlgorithm) {
                        return crypto.decrypt(this.config.connection.password, this.config.connection.cryptoAlgorithm);
                    }
                    return false;
                })
                .then((r) => {
                    this.config.connection.password = (r || this.config.connection.password);
                    return this.tryConnect();
                })
                .then(this.refreshView.bind(this, true))
                .then(this.loadSchema.bind(this, null))
                .then(this.updateSchema.bind(this, {paths: 'schema', retry: this.config.retrySchemaUpdate, load: true}))
                .then(this.refreshView.bind(this, false))
                .then(this.linkSP.bind(this))
                .then(this.doc.bind(this))
                .then((v) => { this.connectionReady = true; return v; })
                .then(this.updateSchema.bind(this, {paths: 'seed', retry: false, load: false}))
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
        }
        start() {
            this.cbc = this.config.cbc && crypto.cbc(this.config.cbc);
            this.bus && this.bus.attachHandlers(this.methods, this.config.imports);
            this.methods.importedMap && Array.from(this.methods.importedMap.values()).forEach(value => {
                if (Array.isArray(value.skipTableType)) {
                    this.config.skipTableType = this.config.skipTableType.concat(value.skipTableType);
                };
            });
            if (Array.isArray(this.config.createTT)) {
                Object.assign(this.config.tableToType, this.config.createTT.reduce(function(obj, tableName) {
                    obj[tableName.toLowerCase()] = true;
                    return obj;
                }, {}));
            }
            return Promise.resolve()
                .then(() => super.start(...arguments))
                .then(this.connect.bind(this))
                .then(result => {
                    this.pull(this.exec);
                    return result;
                });
        }
        async stop() {
            clearTimeout(this.retryTimeout);
            // this.queue.push();
            this.connectionReady = false;
            let connection = this.connection;
            this.connection = null;
            await (connection && connection.close());
            return super.stop(...arguments);
        }
        checkConnection(checkReady) {
            if (this.config.offline) return;
            if (!this.connection) {
                throw sqlPortErrors['portSQL.noConnection']({
                    server: this.config.connection && this.config.connection.server,
                    database: this.config.connection && this.config.connection.database
                });
            }
            if (checkReady && !this.connectionReady) {
                throw sqlPortErrors['portSQL.notReady']({
                    server: this.config.connection && this.config.connection.server,
                    database: this.config.connection && this.config.connection.database
                });
            }
        }
        /**
             * @function exec
             * @description Handles SQL query execution
             * @param {Object} message
             */
        exec(message) {
            let $meta = (arguments.length && arguments[arguments.length - 1]);
            $meta.debug = !!this.bus.config.debug;
            let methodName = ($meta && $meta.method);
            if (methodName) {
                let parts = methodName.match(/^([^[#?]*)[^[]*(\[[0+?^]?])?$/);
                let modifier;
                if (parts) {
                    methodName = parts[1];
                    modifier = parts[2];
                }
                let method = this.findHandler(methodName);
                if (method instanceof Function) {
                    return Promise.resolve()
                        .then(() => method.apply(this, Array.prototype.slice.call(arguments)))
                        .then(result => {
                            switch (modifier) {
                                case '[]':
                                    if (result && result.length === 1) {
                                        return result[0];
                                    } else {
                                        throw sqlPortErrors['portSQL.singleResultsetExpected']();
                                    }
                                case '[^]':
                                    if (result && result.length === 0) {
                                        return null;
                                    } else {
                                        throw sqlPortErrors['portSQL.noRowsExpected']();
                                    }
                                case '[0]':
                                    if (result && result.length === 1 && result[0] && result[0].length === 1) {
                                        return result[0][0];
                                    } else {
                                        throw sqlPortErrors['portSQL.oneRowExpected']();
                                    }
                                case '[?]':
                                    if (result && result.length === 1 && result[0] && result[0].length <= 1) {
                                        return result[0][0];
                                    } else {
                                        throw sqlPortErrors['portSQL.maxOneRowExpected']();
                                    }
                                case '[+]':
                                    if (result && result.length === 1 && result[0] && result[0].length >= 1) {
                                        return result[0];
                                    } else {
                                        throw sqlPortErrors['portSQL.minOneRowExpected']();
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

            if (!this.config.allowQuery || !message.query) {
                return Promise.reject(this.bus.errors['bus.methodNotFound']({params: {method: methodName}}));
            };

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
                        let error = port.errors.getError(err.message && err.message.split('\n').shift()) || sqlPortErrors.portSQL;
                        reject(error(err));
                    } else {
                        $meta.mtid = 'response';
                        if (message.process === 'return') {
                            if (result && result.recordset && result.recordset.length) {
                                Object.keys(result.recordset[0]).forEach(function(value) {
                                    setPathProperty(message, value, result.recordset[0][value]);
                                });
                            }
                            resolve(message);
                        } else if (message.process === 'json') {
                            message.dataSet = result.recordset;
                            resolve(message);
                        } else if (message.process === 'xls') { // todo
                            reject(sqlPortErrors['portSQL.notImplemented'](message.process));
                        } else if (message.process === 'xml') { // todo
                            reject(sqlPortErrors['portSQL.notImplemented'](message.process));
                        } else if (message.process === 'csv') { // todo
                            reject(sqlPortErrors['portSQL.notImplemented'](message.process));
                        } else if (message.process === 'processRows') { // todo
                            reject(sqlPortErrors['portSQL.notImplemented'](message.process));
                        } else if (message.process === 'queueRows') { // todo
                            reject(sqlPortErrors['portSQL.notImplemented'](message.process));
                        } else {
                            reject(sqlPortErrors['portSQL.missingProcess'](message.process));
                        }
                    }
                });
            });
        }
        getPaths(name) {
            let result = [];
            if (this.config[name]) {
                let folder;
                if (typeof (this.config[name]) === 'function') {
                    folder = this.config[name]();
                } else {
                    folder = this.config[name];
                }
                if (Array.isArray(folder)) {
                    result = folder.slice();
                } else {
                    result.push({path: folder});
                }
            }
            this.methods.importedMap && Array.from(this.methods.importedMap.entries()).forEach(function([imported, value]) {
                if (this.includesConfig('updates', imported, true)) {
                    value[name] && Array.prototype.push.apply(result, value[name]);
                }
            }.bind(this));
            return result.reduce((all, item) => {
                item = (typeof item === 'function') ? item(this) : item;
                item && all.push(item);
                return all;
            }, []);
        }
        updateSchema({paths, retry, load}, schema) {
            this.checkConnection();
            let busConfig = this.bus.config;

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

            const preProcess = (statement, fileName, objectName) => {
                statement = interpolate(statement, busConfig);

                if (this.cbc) {
                    statement = statement.replace(ENCRYPT_RE, (match, value) => '0x' + this.cbc.encrypt(value).toString('hex'));
                }

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
            };

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
                    statement = interpolate(statement, busConfig);
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
                    statement = interpolate(statement, busConfig);
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

            const addSP = (queries, {fileName, objectName, objectId, config}) => {
                const params = require(fileName);
                queries.push({
                    fileName,
                    objectName,
                    objectId,
                    callSP: () => this.methods[objectName].call(
                        this,
                        typeof params === 'function' ? params(config) : params,
                        {
                            auth: {
                                actorId: 0
                            },
                            method: objectName,
                            userName: 'SYSTEM'
                        })
                });
            };

            function getObjectName(fileName) {
                return fileName.replace(/\.(sql|js|json)$/i, '').replace(/^[^$-]*[$-]/, ''); // remove "prefix[$-]" and ".sql/.js/.json" suffix
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
                                .then(() => {
                                    self.log.warn && self.log.warn({
                                        fileName: schema.fileName,
                                        $meta: {
                                            opcode: 'portSQL.retrySuccess'
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
                            throw sqlPortErrors['portSQL.retryFailedSchemas'](errCollection);
                        }
                        return retrySchemaUpdate(newFailedQueue);
                    });
            }

            let self = this;
            let folders = this.getPaths(paths);
            let failedQueries = [];
            let hashDropped = false;
            if (!folders || !folders.length) {
                return schema;
            }
            return new Promise((resolve, reject) => {
                let objectList = [];
                let promise = Promise.resolve();
                folders.forEach((schemaConfig) => {
                    promise = promise
                        .then(() => {
                            return new Promise((resolve, reject) => {
                                fs.readdir(schemaConfig.path, (err, files) => {
                                    if (err) {
                                        reject(err);
                                        return;
                                    }
                                    let queries = [];
                                    files = files.sort().map(file => {
                                        return {
                                            originalName: file,
                                            name: interpolate(file, busConfig)
                                        };
                                    });
                                    if (schemaConfig.exclude && schemaConfig.exclude.length > 0) {
                                        files = files.filter((file) => !(schemaConfig.exclude.indexOf(file.name) >= 0));
                                    }
                                    let objectIds = files.reduce(function(prev, cur) {
                                        prev[getObjectName(cur.name).toLowerCase()] = true;
                                        return prev;
                                    }, {});
                                    files.forEach(function(file) {
                                        let objectName = getObjectName(file.name);
                                        let objectId = objectName.toLowerCase();
                                        let fileName = path.join(schemaConfig.path, file.originalName);
                                        if (!fs.statSync(fileName).isFile()) {
                                            return;
                                        }
                                        switch (path.extname(fileName).toLowerCase()) {
                                            case '.sql':
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
                                                };
                                                break;
                                            case '.js':
                                            case '.json':
                                                addSP(queries, {
                                                    fileName: fileName,
                                                    objectName: objectName,
                                                    objectId: objectId,
                                                    config: schemaConfig.config
                                                });
                                                break;
                                            default:
                                                throw new Error('Unsupported file extension: ' + fileName);
                                        };
                                    });

                                    let request = self.getRequest();
                                    let updated = [];
                                    let innerPromise = Promise.resolve();
                                    if (queries.length && !hashDropped) {
                                        innerPromise = innerPromise
                                            .then(() => request.batch(mssqlQueries.dropHash())
                                                .then(() => {
                                                    hashDropped = true;
                                                    return true;
                                                }));
                                    }
                                    queries.forEach((query) => {
                                        innerPromise = innerPromise.then(() => {
                                            let operation = query.callSP ? query.callSP() : request.batch(query.content);
                                            return operation
                                                .then(() => updated.push(query.objectName))
                                                .catch((err) => {
                                                    err.message = err.message + ' (' + query.fileName + ':' + (err.lineNumber || 1) + ':1)';
                                                    let newError = sqlPortErrors['portSQL.updateSchema'](err);
                                                    newError.fileName = query.fileName;
                                                    if (!retry) {
                                                        throw newError;
                                                    } else {
                                                        failedQueries.push(query);
                                                        self.log.error && self.log.error(newError);
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
                                                    opcode: 'update.' + paths
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
                    if (!load) return schema;
                    return self.loadSchema(objectList);
                });
        }
        execTemplate(template, params) {
            let self = this;
            return template.render(params).then(function(query) {
                return self.exec({query: query, process: 'json'})
                    .then(result => result && result.dataSet);
            });
        }
        execTemplateRow(template, params) {
            return this.execTemplate(template, params).then(function(data) {
                let result = (data && data[0]) || {};
                if (result._errorCode && parseInt(result._errorCode, 10) !== 0) {
                    // throw error if _errorCode is '', undefined, null, number (different than 0) or string (different than '0', '00', etc.)
                    let error = sqlPortErrors.portSQL({
                        code: result._errorCode || -1,
                        message: result._errorMessage || 'sql error'
                    });
                    throw error;
                } else {
                    return result;
                }
            });
        }
        execTemplateRows(template, params) {
            return this.execTemplate(template, params).then(function(data) {
                let result = data || [{}];
                if (result[0] && result[0]._errorCode && parseInt(result[0]._errorCode, 10) !== 0) {
                    // throw error if _errorCode is '', undefined, null, number (different than 0) or string (different than '0', '00', etc.)
                    let error = sqlPortErrors.portSQL({
                        code: result[0]._errorCode || -1,
                        message: result[0]._errorMessage || 'sql error'
                    });
                    throw error;
                } else {
                    return result;
                }
            });
        }
        getRequest() {
            let request = new mssql.Request(this.connection);
            request.on('info', (info) => {
                this.log.warn && this.log.warn({ $meta: { mtid: 'event', opcode: 'message' }, message: info });
            });
            return request;
        }
        callSP(name, params, flatten, fileName) {
            let self = this;
            let nesting = this.config.maxNesting;
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
                function recurse(cur, prop, depth) {
                    if (depth > nesting) throw new Error('Unsupported deep nesting for property ' + prop);
                    if (typeof cur === 'function') {
                    } else if (Object(cur) !== cur) {
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
                            recurse(cur[p], prop ? prop + delimiter + p : p, depth + 1);
                        }
                        if (isEmpty && prop) {
                            result[prop] = {};
                        }
                    }
                }
                recurse(data, '', 1);
                return result;
            }
            function getValue(column, value, def, updated) {
                if (updated) {
                    return updated;
                }
                if (value === undefined) {
                    return def;
                } else if (value) {
                    if (self.cbc && isEncrypted({name: column.name, def: {type: column.type.declaration, size: column.length}})) {
                        if (!Buffer.isBuffer(value) && typeof value === 'object') value = JSON.stringify(value);
                        return self.cbc.encrypt(Buffer.from(value));
                    } else if (/^(date.*|smalldate.*)$/.test(column.type.declaration)) {
                        // set a javascript date for 'date', 'datetime', 'datetime2' 'smalldatetime'
                        return new Date(value);
                    } else if (column.type.declaration === 'time') {
                        return new Date('1970-01-01T' + value + 'Z');
                    } else if (column.type.declaration === 'xml') {
                        let obj = {};
                        obj[column.name] = value;
                        return xmlBuilder.buildObject(obj);
                    } else if (value.type === 'Buffer') {
                        return Buffer.from(value.data);
                    } else if (typeof value === 'object') {
                        return JSON.stringify(value);
                    }
                }
                return value;
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
                        value = Object.assign({}, $meta.forward, $meta);
                    } else if (param.update) {
                        value = data[param.name] || data.hasOwnProperty(param.update);
                    } else {
                        value = data[param.name];
                    }
                    if (param.encrypt && value != null) {
                        value = self.cbc.encrypt(Buffer.from(value));
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
                    var filename = typeof $meta.saveAs === 'string' ? $meta.saveAs : $meta.saveAs.filename;
                    if (path.isAbsolute(filename)) {
                        throw sqlPortErrors['portSQL.absolutePath']();
                    }
                    let baseDir = path.join(this.bus.config.workDir, 'ut-port-sql', 'export');
                    let newFilename = path.resolve(baseDir, filename);
                    if (!newFilename.startsWith(baseDir)) {
                        return Promise.reject(sqlPortErrors['portSQL.invalidFileLocation']());
                    }
                    return new Promise((resolve, reject) => {
                        fsplus.makeTree(path.dirname(newFilename), (err) => {
                            if (!err || err.code === 'EEXIST') {
                                return resolve();
                            }
                            return reject(err);
                        });
                    })
                        .then(function(resolve, reject) {
                            request.stream = true;
                            let ws = fs.createWriteStream(newFilename);
                            saveAs(request, $meta.saveAs).pipe(ws);
                            request.execute(name);
                            return new Promise(function(resolve, reject) {
                                ws.on('finish', function() {
                                    return resolve({outputFilePath: newFilename});
                                });
                                ws.on('error', function(err) {
                                    return reject(err);
                                });
                            });
                        });
                }
                if (this.config.offline) {
                    // todo offline exec
                    return [];
                }
                return request.execute(name)
                    .then(function(result) {
                        let promise = Promise.resolve();
                        result.recordsets.forEach(function(resultset) {
                            const encryptedColumns = [];
                            const xmlColumns = [];
                            Object.keys(resultset.columns).forEach(column => {
                                switch (resultset.columns[column].type.declaration) {
                                    case 'varbinary':
                                        if (self.cbc && isEncrypted(resultset.columns[column])) {
                                            encryptedColumns.push(column);
                                        }
                                        break;
                                    case 'xml':
                                        xmlColumns.push(column);
                                        break;
                                    default:
                                        break;
                                }
                            });
                            if (xmlColumns.length || encryptedColumns.length) {
                                resultset.forEach(function(record) {
                                    encryptedColumns.forEach(function(key) {
                                        if (record[key]) { // value is not null
                                            record[key] = self.cbc.decrypt(record[key]);
                                        }
                                    });
                                    xmlColumns.forEach(function(key) {
                                        if (record[key]) { // value is not null
                                            promise = promise
                                                .then(function() {
                                                    return new Promise(function(resolve, reject) {
                                                        xmlParser.parseString(record[key], function(err, result) {
                                                            if (err) {
                                                                reject(sqlPortErrors['portSQL.wrongXmlFormat']({
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
                        return promise.then(() => result.recordsets);
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
                                        throw sqlPortErrors['portSQL.invalidResultSetOrder']({
                                            expectName: true
                                        });
                                    } else {
                                        name = resultSets[i][0].resultSetName;
                                        single = !!resultSets[i][0].single;
                                        if (name === 'ut-error') {
                                            error = self.errors.getError(resultSets[i][0] && resultSets[i][0].type) || sqlPortErrors.portSQL;
                                            error = Object.assign(error(), resultSets[i][0]);
                                            name = null;
                                            single = false;
                                        }
                                    }
                                } else {
                                    if (isNamingResultSet(resultSets[i])) {
                                        throw sqlPortErrors['portSQL.invalidResultSetOrder']({
                                            expectName: false
                                        });
                                    }
                                    if (namedSet.hasOwnProperty(name)) {
                                        throw sqlPortErrors['portSQL.duplicateResultSetName']({
                                            name: name
                                        });
                                    }
                                    if (single) {
                                        if (resultSets[i].length === 0) {
                                            namedSet[name] = null;
                                        } else if (resultSets[i].length === 1) {
                                            namedSet[name] = resultSets[i][0];
                                        } else {
                                            throw sqlPortErrors['portSQL.singleResultExpected']({
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
                                throw sqlPortErrors['portSQL.invalidResultSetOrder']({
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
                        let error = self.errors.getError(err.type || err.message) || sqlPortErrors.portSQL;
                        if (error.type === err.message) {
                            // use default message
                            delete err.message;
                        }
                        let errToThrow = error(err);
                        if (debug) {
                            err.storedProcedure = name;
                            err.params = debugParams;
                            err.fileName = (fileName || name) + ':' + err.lineNumber + ':1';
                            let stack = errToThrow.stack.split('\n');
                            stack.splice.apply(stack, [1, 0].concat(errorLines));
                            errToThrow.stack = stack.join('\n');
                        }
                        throw errToThrow;
                    });
            };
        }
        linkSP(schema) {
            if (schema.parseList.length) {
                let parserSP = require('./parsers/mssqlSP');
                schema.parseList.forEach(function(procedure) {
                    let binding;
                    try {
                        binding = parserSP.parse(procedure.source);
                    } catch (e) {
                        if (this.isDebug()) {
                            e.source = procedure.source;
                        }
                        throw e;
                    }
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
                        binding.params && binding.params.forEach(param => {
                            (update.indexOf(param.name) >= 0) && (param.update = param.name.replace(/\$update$/i, ''));
                            if (isEncrypted(param) && this.cbc) {
                                param.encrypt = true;
                            };
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
                                            throw sqlPortErrors['portSQL.unexpectedType']({
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
                        this.methods[flatName] = this.callSP(binding.name, binding.params, flatten, procedure.fileName);
                    }
                }.bind(this));
            }
            return schema;
        }
        loadSchema(objectList, hash) {
            let self = this;
            let schema = this.getPaths('schema');
            let cacheFile = name => path.join(this.bus.config.workDir, 'ut-port-sql', name ? name + '.json' : '');
            if (hash) {
                let cacheFileName = cacheFile(hash);
                if (fs.existsSync(cacheFileName)) {
                    return serverRequire(cacheFileName);
                }
            }
            if (((Array.isArray(schema) && !schema.length) || !schema) && !this.config.linkSP) {
                return { source: {}, parseList: [] };
            }
            this.checkConnection();
            let request = this.getRequest();
            request.multiple = true;
            return request.query(mssqlQueries.loadSchema(this.config.updates === false || this.config.updates === 'false')).then(function(result) {
                let schema = {source: {}, parseList: [], types: {}, deps: {}};
                result.recordsets[0].reduce(function(prev, cur) { // extract source code of procedures, views, functions, triggers
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
                result.recordsets[1].reduce(function(prev, cur) { // extract columns of user defined table types
                    let parserDefault = require('./parsers/mssqlDefault');
                    changeRowVersionType(cur);
                    if (!(mssql[cur.type.toUpperCase()] instanceof Function)) {
                        throw sqlPortErrors['portSQL.unexpectedColumnType']({
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
                        throw sqlPortErrors['portSQL.parserError'](err);
                    }
                    let type = prev[cur.name] || (prev[cur.name] = []);
                    type.push(cur);
                    return prev;
                }, schema.types);
                result.recordsets[2].reduce(function(prev, cur) { // extract dependencies
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
            })
                .then(schema => {
                    if (objectList && self.config.cache) {
                        let content = stringify(schema);
                        let contentHash = crypto.hash(content);
                        fsplus.makeTreeSync(cacheFile());
                        fs.writeFileSync(cacheFile(contentHash), content);
                        return request.query(mssqlQueries.setHash(contentHash))
                            .then(() => schema);
                    } else {
                        return schema;
                    }
                });
        }
        refreshView(drop, data) {
            this.checkConnection();
            if (this.config.offline) return drop ? this.config.offline : data;
            let schema = this.getPaths('schema');
            if ((Array.isArray(schema) && !schema.length) || !schema) {
                if (drop && this.config.cache) {
                    return this.getRequest()
                        .query(mssqlQueries.getHash())
                        .then(result => result && result.recordset && result.recordset[0] && result.recordset[0].hash);
                }
                return !drop && data;
            }
            return this.getRequest()
                .query(mssqlQueries.refreshView(drop))
                .then(result => {
                    if (!drop && result && result.recordset && result.recordset.length && result.recordset[0].view_name) {
                        throw sqlPortErrors['portSQL.invalidView'](result);
                    }
                    if (this.config.cache && drop && result && result.recordset && result.recordset[0] && result.recordset[0].hash) {
                        return result.recordset[0].hash;
                    }
                    return !drop && data;
                });
        }
        doc(schema) {
            if (!this.config.doc) {
                return schema;
            }
            this.checkConnection();
            let self = this;
            let schemas = this.getPaths('schema');
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
        }
        tryConnect() {
            if (this.config.offline) return;
            if (this.config.connection) {
                this.config.connection.beforeConnect = c => {
                    if (c.debug) {
                        let id = (++this.connectionAttempt);
                        let created = new Date();
                        let context = {id, created};
                        let notify = (event, connection) => {
                            this.log.info && this.log.info({$meta: {mtid: 'event', opcode: 'port.pool.' + event}, connection});
                        };
                        c.debug.packet = (direction, packet) => {
                            if (direction === 'Sent') {
                                let length = packet.length();
                                this.bytesSent && this.bytesSent(length + 8);
                                if (this.log.trace) {
                                    let id = packet.packetId();
                                    if (id === 255 || packet.isLast()) {
                                        this.log.trace({
                                            $meta: {mtid: 'event', opcode: 'port.pool.out'},
                                            message: {
                                                size: length + id * c.messageIo.packetSize(),
                                                header: packet.headerToString()
                                            }
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
                                            $meta: {mtid: 'event', opcode: 'port.pool.in'},
                                            message: {
                                                size: length + id * c.messageIo.packetSize(),
                                                header: packet.headerToString()
                                            }
                                        });
                                    }
                                }
                            }
                        };
                        c.debug.log = msg => {
                            if (this.log.debug && c.state && (c.state.name !== 'LoggedIn') && (c.state.name !== 'SentClientRequest')) {
                                this.log.debug({$meta: {mtid: 'event', opcode: 'port.pool.state'}, message: {state: msg, id, created}});
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

            const toInt = value => {
                value = parseInt(value, 10);
                return Number.isInteger(value) ? value : undefined;
            };

            let sanitize = options => ({
                ...options,
                ...{
                    requestTimeout: toInt(options.requestTimeout),
                    connectionTimeout: toInt(options.connectionTimeout)
                }
            });

            this.connection = new mssql.ConnectionPool(sanitize(this.config.connection));
            if (this.config.create && this.config.create.user) {
                let conCreate = new mssql.ConnectionPool(
                    sanitize({...this.config.connection, ...{user: '', password: '', database: ''}, ...this.config.create}) // expect explicit user/pass
                );

                // Patch for https://github.com/patriksimek/node-mssql/issues/467
                conCreate._throwingClose = conCreate._close;
                conCreate._close = function(callback) {
                    const close = conCreate._throwingClose.bind(this, callback);
                    if (this.pool) {
                        return this.pool.drain().then(close);
                    } else {
                        return close();
                    }
                };
                // end patch

                return conCreate.connect()
                    .then(() => (new mssql.Request(conCreate)).batch(mssqlQueries.createDatabase(this.config.connection.database)))
                    .then(() => this.config.create.diagram && new mssql.Request(conCreate).batch(mssqlQueries.enableDatabaseDiagrams(this.config.connection.database)))
                    .then(() => {
                        if (this.config.create.user === this.config.connection.user) {
                            return;
                        }
                        return (new mssql.Request(conCreate)).batch(mssqlQueries.createUser(this.config.connection.database, this.config.connection.user, this.config.connection.password));
                    })
                    .then(() => conCreate.close())
                    .then(() => this.connection.connect())
                    .catch((err) => {
                        this.log && this.log.error && this.log.error(err);
                        try { conCreate.close(); } catch (e) {};
                        throw err;
                    });
            } else {
                return this.connection.connect();
            }
        }
    };

    function fieldSource(column) {
        return (column.column + '\t' + column.type + '\t' + (column.length === null ? '' : column.length) + '\t' + (column.scale === null ? '' : column.scale)).toLowerCase();
    }

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
};
