'use strict';
const errors = require('./errors.json');
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
const saveAs = require('./saveAs');
const ROW_VERSION_INNER_TYPE = 'BINARY';
const {setParam, isEncrypted, flattenMessage} = require('./params');

function changeRowVersionType(field) {
    if (field && (field.type.toUpperCase() === 'ROWVERSION' || field.type.toUpperCase() === 'TIMESTAMP')) {
        field.type = ROW_VERSION_INNER_TYPE;
        field.length = 8;
    }
}

module.exports = function({utPort, registerErrors, vfs}) {
    if (!vfs) throw new Error('ut-run@10.19.0 or newer is required');
    return class SqlPort extends utPort {
        constructor() {
            super(...arguments);
            if (!this.errors || !this.errors.getError) throw new Error('Please use the latest version of ut-port');
            Object.assign(this.errors, registerErrors(errors));
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
                allowQuery: false,
                retry: 10000,
                paramsOutName: 'out',
                doc: false,
                maxNesting: 5,
                cbcStable: {},
                cbcDate: {},
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

        get schema() {
            return {
                type: 'object',
                properties: {
                    logLevel: {
                        type: 'string',
                        enum: ['error', 'warning', 'info', 'debug', 'trace'],
                        default: 'info'
                    },
                    connection: {
                        type: 'object',
                        properties: {
                            server: {
                                type: 'string'
                            },
                            database: {
                                type: 'string'
                            },
                            user: {
                                type: 'string'
                            },
                            password: {
                                type: 'string'
                            },
                            connectionTimeout: {
                                type: ['integer', 'null'],
                                title: 'Connection timeout (ms)'
                            },
                            requestTimeout: {
                                type: ['integer', 'null'],
                                title: 'Request timeout (ms)'
                            }
                        },
                        required: ['server', 'database', 'user', 'password']
                    }
                },
                required: ['connection']
            };
        }

        get uiSchema() {
            return {
                connection: {
                    password: {
                        'ui:widget': 'password'
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
            if (this.config.cbc) {
                const {encrypt, decrypt} = crypto.cbc(this.config.cbc);
                this.cbc = {
                    encrypt: (value, field) => encrypt(
                        this.config.cbcDate[field]
                            ? new Date(value).getTime().toString()
                            : value,
                        field && (field === true || this.config.cbcStable[field])
                    ),
                    decrypt: (value, field) => {
                        value = decrypt(value, field && this.config.cbcStable[field]);
                        return this.config.cbcDate[field] ? new Date(parseInt(value)) : value;
                    }
                };
            }
            this.hmac = this.config.hmac && crypto.hmac(this.config.hmac);
            this.bus && this.bus.attachHandlers(this.methods, this.config.imports);
            if (this.config.createTT != null) this.log.warn && this.log.warn('Property createTT should be moved in schema array');
            this.methods.importedMap && Array.from(this.methods.importedMap.values()).forEach(value => {
                if (Array.isArray(value.skipTableType)) {
                    this.log.warn && this.log.warn('Property skipTableType should be moved in schema array');
                    value.schema.forEach(schema => {
                        schema.skipTableType = (schema.skipTableType || []).concat(value.skipTableType);
                    });
                };
                if (this.config.createTT != null && value.schema) {
                    value.schema.forEach(schema => {
                        if (schema.createTT == null) schema.createTT = this.config.createTT;
                    });
                }
                if (Array.isArray(value.cbcStable)) value.cbcStable.forEach(key => { this.config.cbcStable[key] = true; });
                if (Array.isArray(value.cbcDate)) value.cbcDate.forEach(key => { this.config.cbcDate[key] = true; });
            });
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
            const connection = this.connection;
            this.connection = null;
            await (connection && connection.close());
            return super.stop(...arguments);
        }

        checkConnection(checkReady) {
            if (this.config.offline) return;
            if (!this.connection) {
                throw this.errors['portSQL.noConnection']({
                    server: this.config.connection && this.config.connection.server,
                    database: this.config.connection && this.config.connection.database
                });
            }
            if (checkReady && !this.connectionReady) {
                throw this.errors['portSQL.notReady']({
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
            const $meta = (arguments.length && arguments[arguments.length - 1]);
            $meta.debug = !!this.bus.config.debug;
            let methodName = ($meta && $meta.method);
            if (methodName) {
                const parts = methodName.match(/^([^[#?]*)[^[]*(\[[0+?^]?])?$/);
                let modifier;
                if (parts) {
                    methodName = parts[1];
                    modifier = parts[2];
                }
                const method = this.findHandler(methodName);
                if (method instanceof Function) {
                    return Promise.resolve()
                        .then(() => method.apply(this, Array.prototype.slice.call(arguments)))
                        .then(result => {
                            switch (modifier) {
                                case '[]':
                                    if (result && result.length === 1) {
                                        return result[0];
                                    } else {
                                        throw this.errors['portSQL.singleResultsetExpected']();
                                    }
                                case '[^]':
                                    if (result && result.length === 0) {
                                        return null;
                                    } else {
                                        throw this.errors['portSQL.noRowsExpected']();
                                    }
                                case '[0]':
                                    if (result && result.length === 1 && result[0] && result[0].length === 1) {
                                        return result[0][0];
                                    } else {
                                        throw this.errors['portSQL.oneRowExpected']();
                                    }
                                case '[?]':
                                    if (result && result.length === 1 && result[0] && result[0].length <= 1) {
                                        return result[0][0];
                                    } else {
                                        throw this.errors['portSQL.maxOneRowExpected']();
                                    }
                                case '[+]':
                                    if (result && result.length === 1 && result[0] && result[0].length >= 1) {
                                        return result[0];
                                    } else {
                                        throw this.errors['portSQL.minOneRowExpected']();
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

            const debug = this.isDebug();
            const request = this.getRequest();
            const port = this;
            return new Promise(function(resolve, reject) {
                request.query(message.query, function(err, result) {
                    // let end = +new Date();
                    // let execTime = end - start;
                    // todo record execution time
                    if (err) {
                        debug && (err.query = message.query);
                        const error = port.errors.getError(err.message && err.message.split('\n').shift()) || port.errors.portSQL;
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
                            reject(port.errors['portSQL.notImplemented'](message.process));
                        } else if (message.process === 'xml') { // todo
                            reject(port.errors['portSQL.notImplemented'](message.process));
                        } else if (message.process === 'csv') { // todo
                            reject(port.errors['portSQL.notImplemented'](message.process));
                        } else if (message.process === 'processRows') { // todo
                            reject(port.errors['portSQL.notImplemented'](message.process));
                        } else if (message.process === 'queueRows') { // todo
                            reject(port.errors['portSQL.notImplemented'](message.process));
                        } else {
                            reject(port.errors['portSQL.missingProcess'](message.process));
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
            const busConfig = this.bus.config;

            function retrySchemaUpdate(failedQueue) {
                const newFailedQueue = [];
                const request = self.getRequest();
                const errCollection = [];
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
                                            mtid: 'event',
                                            method: 'portSQL.retrySuccess'
                                        }
                                    });
                                    return true;
                                })
                                .catch((err) => {
                                    const newErr = err;
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
                            throw self.errors['portSQL.retryFailedSchemas'](errCollection);
                        }
                        return retrySchemaUpdate(newFailedQueue);
                    });
            }

            const self = this;
            const folders = this.getPaths(paths);
            const failedQueries = [];
            let hashDropped = false;
            if (!folders || !folders.length) {
                return schema;
            }
            const processFiles = require('./processFiles');
            return folders.reduce((promise, schemaConfig) =>
                promise.then(allDbObjects =>
                    new Promise((resolve, reject) => {
                        vfs.readdir(schemaConfig.path, (err, files) => {
                            if (err) {
                                reject(err);
                                return;
                            }
                            const {queries, dbObjects} = processFiles(schema, busConfig, schemaConfig, files, vfs, this.cbc);

                            const request = self.getRequest();
                            const updated = [];
                            let innerPromise = Promise.resolve();
                            if (queries.length && !hashDropped && load) {
                                innerPromise = innerPromise
                                    .then(() => request.batch(mssqlQueries.dropHash())
                                        .then(() => {
                                            hashDropped = true;
                                            return true;
                                        }));
                            }
                            queries.forEach((query) => {
                                innerPromise = innerPromise.then(() => {
                                    const operation = query.callSP ? query.callSP.apply(self) : request.batch(query.content);
                                    return operation
                                        .then(() => updated.push(query.objectName))
                                        .catch((err) => {
                                            err.message = err.message + ' (' + query.fileName + ':' + (err.lineNumber || 1) + ':1)';
                                            const newError = self.errors['portSQL.updateSchema'](err);
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
                                            method: 'update.' + paths
                                        }
                                    });
                                    return resolve({...allDbObjects, ...dbObjects});
                                })
                                .catch(reject);
                        });
                    })
                ), Promise.resolve({}))
                .then((objectList) => {
                    if (!failedQueries.length) {
                        return objectList;
                    }
                    return retrySchemaUpdate(failedQueries)
                        .then(() => (objectList));
                })
                .then(function(objectList) {
                    if (!load || self.config.offline) return schema;
                    return self.loadSchema(objectList, false, hashDropped);
                });
        }

        execTemplate(template, params) {
            const self = this;
            return template.render(params).then(function(query) {
                return self.exec({query: query, process: 'json'})
                    .then(result => result && result.dataSet);
            });
        }

        execTemplateRow(template, params) {
            return this.execTemplate(template, params).then(data => {
                const result = (data && data[0]) || {};
                if (result._errorCode && parseInt(result._errorCode, 10) !== 0) {
                    // throw error if _errorCode is '', undefined, null, number (different than 0) or string (different than '0', '00', etc.)
                    const error = this.errors.portSQL({
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
            return this.execTemplate(template, params).then(data => {
                const result = data || [{}];
                if (result[0] && result[0]._errorCode && parseInt(result[0]._errorCode, 10) !== 0) {
                    // throw error if _errorCode is '', undefined, null, number (different than 0) or string (different than '0', '00', etc.)
                    const error = this.errors.portSQL({
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
            const request = new mssql.Request(this.connection);
            request.on('info', (info) => {
                this.log.warn && this.log.warn({ $meta: { mtid: 'event', method: 'mssql.message' }, message: info });
            });
            return request;
        }

        getRowTransformer(columns = {}) {
            if (columns.resultSetName) return;
            let isAsync = false;
            const pipeline = [];
            Object.entries(columns).forEach(([key, column]) => {
                if (column.type.declaration.toUpperCase() === ROW_VERSION_INNER_TYPE) {
                    pipeline.push(record => {
                        if (record[key]) { // value is not null
                            record[key] = record[key].toString('hex');
                        }
                    });
                } else if (column.type.declaration === 'varbinary') {
                    if (this.cbc && isEncrypted(column)) {
                        pipeline.push(record => {
                            if (record[key]) { // value is not null
                                record[key] = this.cbc.decrypt(record[key], column.name);
                            }
                        });
                    }
                } else if (column.type.declaration === 'xml') {
                    isAsync = true;
                    pipeline.push(record => {
                        if (record[key]) { // value is not null
                            return new Promise((resolve, reject) => {
                                xmlParser.parseString(record[key], (err, result) => {
                                    if (err) {
                                        reject(this.errors['portSQL.wrongXmlFormat']({
                                            xml: record[key]
                                        }));
                                    } else {
                                        record[key] = result;
                                        resolve();
                                    }
                                });
                            });
                        }
                    });
                }
                if (/\.json$/i.test(key)) {
                    pipeline.push(record => {
                        record[key.substr(0, key.length - 5)] = record[key] ? JSON.parse(record[key]) : record[key];
                        delete record[key];
                    });
                };
            });
            if (pipeline.length) {
                if (isAsync) {
                    return async record => {
                        for (let i = 0, n = pipeline.length; i < n; i += 1) {
                            await pipeline[i](record);
                        }
                        return record;
                    };
                } else {
                    return record => {
                        pipeline.forEach(fn => fn(record));
                        return record;
                    };
                }
            }
        }

        callSP(name, params, flatten, fileName, method) {
            const self = this;
            const nesting = this.config.maxNesting;
            const outParams = [];
            const ngram = params.find(param => param.name === 'ngram' && param.def.type === 'table');
            params && params.forEach(function(param) {
                param.out && outParams.push(param.name);
            });

            return function callLinkedSP(msg, $meta) {
                self.checkConnection(true);
                const request = self.getRequest();
                const data = flattenMessage(msg, flatten, nesting);
                const debug = this.isDebug();
                const debugParams = {};
                const ngramParam = ngram && ngram.def.create();
                request.multiple = true;
                $meta.globalId = uuid.v1();
                params && params.forEach(function(param) {
                    let value;
                    if (param.name === 'meta') {
                        value = Object.assign(
                            {},
                            $meta.forward,
                            $meta,
                            $meta.auth && {auth: null, 'auth.actorId': $meta.auth.actorId, 'auth.sessionId': $meta.auth.sessionId}
                        );
                    } else if (param.def && param.def.typeName && param.def.typeName.endsWith('.ngramTT')) {
                        value = param.options && Object.keys(param.options).map(name => data[name] && [name, data[name]]).filter(Boolean);
                    } else if (param.update) {
                        value = data[param.name] || Object.prototype.hasOwnProperty.call(data, param.update);
                    } else {
                        value = data[param.name];
                    }
                    value = setParam(self.cbc, self.hmac, ngram && {
                        options: ngram.options,
                        add: (...params) => ngramParam.rows.add(...params)
                    }, request, param, value, nesting);
                    debug && (debugParams[param.name] = value);
                });
                if (ngramParam) request.input('ngram', ngramParam);

                if ($meta.saveAs) return saveAs(self, request, $meta, name);

                if (this.config.offline) return []; // todo offline exec

                return request.execute(name)
                    .then(async({recordsets}) => {
                        for (let i = 0, n = recordsets.length; i < n; i += 1) {
                            const transform = self.getRowTransformer(recordsets[i].columns);
                            if (typeof transform === 'function') {
                                if (transform.constructor.name === 'AsyncFunction') {
                                    for (let j = 0, m = recordsets[i].length; j < m; j += 1) {
                                        await transform(recordsets[i][j]);
                                    }
                                } else {
                                    recordsets[i].forEach(transform);
                                }
                            }
                        }
                        return recordsets;
                    })
                    .then(function(resultSets) {
                        function isNamingResultSet(element) {
                            return Array.isArray(element) &&
                                element.length === 1 &&
                                Object.prototype.hasOwnProperty.call(element[0], 'resultSetName') &&
                                typeof element[0].resultSetName === 'string';
                        }
                        let error;
                        if (resultSets.length > 0 && isNamingResultSet(resultSets[0])) {
                            const namedSet = {};
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
                                        throw self.errors['portSQL.invalidResultSetOrder']({
                                            expectName: true
                                        });
                                    } else {
                                        name = resultSets[i][0].resultSetName;
                                        single = !!resultSets[i][0].single;
                                        if (name === 'ut-error') {
                                            error = self.errors.getError(resultSets[i][0] && resultSets[i][0].type) || self.errors.portSQL;
                                            error = Object.assign(error(), resultSets[i][0]);
                                            name = null;
                                            single = false;
                                        }
                                    }
                                } else {
                                    if (isNamingResultSet(resultSets[i])) {
                                        throw self.errors['portSQL.invalidResultSetOrder']({
                                            expectName: false
                                        });
                                    }
                                    if (Object.prototype.hasOwnProperty.call(namedSet, name)) {
                                        throw self.errors['portSQL.duplicateResultSetName']({
                                            name: name
                                        });
                                    }
                                    if (single) {
                                        if (resultSets[i].length === 0) {
                                            namedSet[name] = null;
                                        } else if (resultSets[i].length === 1) {
                                            namedSet[name] = resultSets[i][0];
                                        } else {
                                            throw self.errors['portSQL.singleResultExpected']({
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
                                throw self.errors['portSQL.invalidResultSetOrder']({
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
                        const errorLines = err.message && err.message.split('\n');
                        err.message = errorLines.shift();
                        const error = self.errors.getError(err.type || err.message) || self.errors.portSQL;
                        if (error.type === err.message) {
                            // use default message
                            delete err.message;
                        }
                        const errToThrow = error({
                            cause: err,
                            params: {
                                method
                            }
                        });
                        if (debug) {
                            err.storedProcedure = name;
                            err.params = debugParams;
                            err.fileName = (fileName || name) + ':' + err.lineNumber + ':1';
                            const stack = errToThrow.stack.split('\n');
                            stack.splice.apply(stack, [1, 0].concat(errorLines));
                            errToThrow.stack = stack.join('\n');
                        }
                        throw errToThrow;
                    });
            };
        }

        linkSP(schema) {
            if (schema.parseList.length) {
                const parserSP = require('./parsers/mssqlSP');
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
                    const flatName = binding.name.replace(/[[\]]/g, '');
                    if (binding && binding.type === 'procedure') {
                        const update = [];
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
                            if (param.doc && /(^\{.*}$)|(^\[.*]$)/s.test(param.doc.trim())) {
                                param.options = JSON.parse(param.doc);
                                param.doc = param.options.docs;
                            }
                            if (param.def && param.def.type === 'table') {
                                const columns = schema.types[param.def.typeName.toLowerCase()];
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
                                param.def.create = () => {
                                    const table = new mssql.Table(param.def.typeName.toLowerCase());
                                    columns && columns.forEach(column => {
                                        changeRowVersionType(column);
                                        const type = mssql[column.type.toUpperCase()];
                                        if (!(type instanceof Function)) {
                                            throw this.errors['portSQL.unexpectedType']({
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
                        this.methods[flatName] = this.callSP(binding.name, binding.params, flatten, procedure.fileName, flatName);
                    }
                }.bind(this));
            }
            return schema;
        }

        loadSchema(objectList, hash, setHash) {
            const self = this;
            const schema = this.getPaths('schema');
            const cacheFile = name => path.join(this.bus.config.workDir, 'ut-port-sql', name ? name + '.json' : '');
            if (hash) {
                const cacheFileName = cacheFile(hash);
                if (fs.existsSync(cacheFileName)) {
                    return JSON.parse(fs.readFileSync(cacheFileName));
                }
            }
            if (((Array.isArray(schema) && !schema.length) || !schema) && !this.config.linkSP) {
                return { source: {}, parseList: [] };
            }
            this.checkConnection();
            const request = this.getRequest();
            request.multiple = true;
            return request.query(mssqlQueries.loadSchema(this.config.updates === false || this.config.updates === 'false')).then(function(result) {
                const schema = {source: {}, parseList: [], types: {}, deps: {}};
                result.recordsets[0].reduce(function(prev, cur) { // extract source code of procedures, views, functions, triggers
                    const full = cur.full;
                    const namespace = cur.namespace;
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
                    const parserDefault = require('./parsers/mssqlDefault');
                    changeRowVersionType(cur);
                    if (!(mssql[cur.type.toUpperCase()] instanceof Function)) {
                        throw self.errors['portSQL.unexpectedColumnType']({
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
                        throw self.errors['portSQL.parserError'](err);
                    }
                    const type = prev[cur.name] || (prev[cur.name] = []);
                    type.push(cur);
                    return prev;
                }, schema.types);
                result.recordsets[2].reduce(function(prev, cur) { // extract dependencies
                    cur.name = cur.name && cur.name.toLowerCase();
                    cur.type = cur.type && cur.type.toLowerCase();
                    const dep = prev[cur.type] || (prev[cur.type] = {names: [], drop: []});
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
                        const content = stringify(schema);
                        const contentHash = crypto.hash(content);
                        fsplus.makeTreeSync(cacheFile());
                        fs.writeFileSync(cacheFile(contentHash), content);
                        return setHash ? request.query(mssqlQueries.setHash(contentHash)).then(() => schema) : schema;
                    } else {
                        return schema;
                    }
                });
        }

        refreshView(drop, data) {
            this.checkConnection();
            if (this.config.offline) return drop ? this.config.offline : data;
            const schema = this.getPaths('schema');
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
                        throw this.errors['portSQL.invalidView'](result);
                    }
                    if (this.config.cache && drop && result && result.recordset && result.recordset[0] && result.recordset[0].hash) {
                        return result.recordset[0].hash;
                    }
                    return !drop && data;
                });
        }

        doc(schema) {
            if (!this.config.doc || this.config.offline) {
                return schema;
            }
            this.checkConnection();
            const self = this;
            const schemas = this.getPaths('schema');
            const parserSP = require('./parsers/mssqlSP');
            return new Promise(function(resolve, reject) {
                const docList = [];
                let promise = Promise.resolve();
                schemas.forEach(function(schemaConfig) {
                    promise = promise
                        .then(function() {
                            return new Promise(function(resolve, reject) {
                                vfs.readdir(schemaConfig.path, function(err, files) {
                                    if (err) {
                                        return reject(err);
                                    }
                                    files = files.sort();
                                    files.forEach(function(file) {
                                        const fileName = path.join(schemaConfig.path, file);
                                        if (!vfs.isFile(fileName)) {
                                            return;
                                        }
                                        const fileContent = vfs.readFileSync(fileName).toString();
                                        if (fileContent.trim().match(/^(\bCREATE\b|\bALTER\b)\s+PROCEDURE/i) || fileContent.trim().match(/^(\bCREATE\b|\bALTER\b)\s+TABLE/i)) {
                                            const binding = parserSP.parse(fileContent);
                                            if (binding.type === 'procedure') {
                                                binding.params.forEach((param) => {
                                                    if (binding.doc) {
                                                        docList.push({
                                                            type0: 'SCHEMA',
                                                            name0: binding.schema,
                                                            type1: 'PROCEDURE',
                                                            name1: binding.table,
                                                            doc: binding.doc
                                                        });
                                                    }
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
                                                if (binding.doc) {
                                                    docList.push({
                                                        type0: 'SCHEMA',
                                                        name0: binding.schema,
                                                        type1: 'TABLE',
                                                        name1: binding.table,
                                                        doc: binding.doc
                                                    });
                                                }
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
                    const request = self.getRequest();
                    request.multiple = true;
                    const docListParam = new mssql.Table('core.documentationTT');
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
                        const id = (++this.connectionAttempt);
                        const created = new Date();
                        let context = {id, created};
                        const notify = (event, connection) => {
                            this.log.info && this.log.info({$meta: {mtid: 'event', method: 'port.pool.' + event}, connection});
                        };
                        c.debug.packet = (direction, packet) => {
                            if (direction === 'Sent') {
                                const length = packet.length();
                                this.bytesSent && this.bytesSent(length + 8);
                                if (this.log.trace) {
                                    const id = packet.packetId();
                                    if (id === 255 || packet.isLast()) {
                                        this.log.trace({
                                            $meta: {mtid: 'event', method: 'port.pool.out'},
                                            message: {
                                                size: length + id * c.messageIo.packetSize(),
                                                header: packet.headerToString()
                                            }
                                        });
                                    }
                                }
                            }
                            if (direction === 'Received') {
                                const length = packet.length();
                                this.bytesReceived && this.bytesReceived(length + 8);
                                if (this.log.trace) {
                                    const id = packet.packetId();
                                    if (id === 255 || packet.isLast()) {
                                        this.log.trace({
                                            $meta: {mtid: 'event', method: 'port.pool.in'},
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
                                this.log.debug({$meta: {mtid: 'event', method: 'port.pool.state'}, message: {state: msg, id, created}});
                            }
                        };
                        c.once('connect', err => {
                            if (!err) {
                                const stream = c.messageIo.socket;
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

            const sanitize = options => ({
                ...options,
                ...{
                    requestTimeout: toInt(options.requestTimeout),
                    connectionTimeout: toInt(options.connectionTimeout)
                }
            });

            this.connection = new mssql.ConnectionPool(sanitize(this.config.connection));
            if (this.config.create && this.config.create.user) {
                const conCreate = new mssql.ConnectionPool(
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
        const path = fieldName.split('.');
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
