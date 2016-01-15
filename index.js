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
    this.connectionReady = false;
    var self = this;
    this.connection = new mssql.Connection(this.config.db);
    return this.connection.connect()
        .then(this.loadSchema.bind(this))
        .then(this.updateSchema.bind(this))
        .then(this.linkSP.bind(this))
        .then(function(v) { self.connectionReady = true; return v; })
        .catch(function(err) {
            try { this.connection.close(); } catch (e) {};
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
    var methodName = ($meta && $meta.method);
    if (methodName) {
        var method = this.config[methodName];
        if (method instanceof Function) {
            return when.lift(method).apply(this, Array.prototype.slice.call(arguments));
        }
    }

    this.checkConnection(true);

    if (this.config.validate instanceof Function) {
        this.config.validate(message);
    }

    // var start = +new Date();
    var request = this.getRequest();
    return when.promise(function(resolve, reject) {
        request.query(message.query, function(err, result) {
            // var end = +new Date();
            // var execTime = end - start;
            // todo record execution time
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

SqlPort.prototype.updateSchema = function(schema) {
    this.checkConnection();

    function getAlterStatement(statement) {
        return statement.trim().replace(/^CREATE /i, 'ALTER ');
    }

    function getCreateStatement(statement) {
        return statement.trim().replace(/^ALTER /i, 'CREATE ');
    }

    var self = this;
    var schemas = this.config.schema && (Array.isArray(this.config.schema) ? this.config.schema : [{path: this.config.schema}]);
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
    }, [])
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
        if (result._errorCode && parseInt(result._errorCode, 10) !== 0) {
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
        if (result[0] && result[0]._errorCode && parseInt(result[0]._errorCode, 10) !== 0) {
            // throw error if _errorCode is '', undefined, null, number (different than 0) or string (different than '0', '00', etc.)
            throw errors.sql({
                code: result[0]._errorCode || -1,
                message: result[0]._errorMessage || 'sql error'
            });
        } else {
            return result;
        }
    });
};

SqlPort.prototype.getRequest = function() {
    return new mssql.Request(this.connection);
};

SqlPort.prototype.callSP = function(name, params, flatten) {
    var self = this;
    var outParams = [];

    params && params.forEach(function(param) {
        param.out && outParams.push(param.name);
    });

    function sqlType(def) {
        var type;
        if (def.type === 'table') {
            type = def.create();
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

    function flattenMessage(data) {
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
                    recurse(cur[p], prop ? prop + '_' + p : p);
                }
                if (isEmpty && prop) {
                    result[prop] = {};
                }
            }
        }
        recurse(data, '');
        return result;
    }

    return function callLinkedSP(msg) {
        self.checkConnection(true);
        var request = self.getRequest();
        var data = flatten ? flattenMessage(msg) : msg;
        request.multiple = true;
        params && params.forEach(function(param) {
            var value = param.update ? (data[param.name] || data.hasOwnProperty(param.update)) : data[param.name];
            var type = sqlType(param.def);
            if (param.out) {
                request.output(param.name, type, value);
            } else {
                if (param.def && param.def.type === 'table') {
                    value && (value.forEach instanceof Function) && value.forEach(function(row){
                        type.rows.add.apply(type.rows, param.columns.reduce(function(prev, cur) {
                            prev.push(row[cur]);
                            return prev;
                        }, []));
                    });
                    request.input(param.name, type);
                } else {
                    request.input(param.name, type, value);
                }
            }
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
            var flatName = binding.name.replace(/[\[\]]/g, '');
            if (binding && binding.type === 'procedure' && !this.config[flatName]) {
                var update = [];
                var flatten = false;
                binding.params && binding.params.forEach(function(param) {
                    update.push(param.name + '$update');
                    // flatten in case a parameter's name have at least one underscore character surrounded by non underscore characters
                    if (!flatten && param.name.match(/[^_]_[^_]/)) {
                        flatten = true;
                    }
                });
                binding.params && binding.params.forEach(function(param) {
                    (update.indexOf(param.name) >= 0) && (param.update = param.name.replace(/\$update$/i, ''));
                    if (param.def && param.def.type === 'table') {
                        var columns = schema.types[param.def.typeName];
                        param.columns = [];
                        columns.forEach(function(column) {
                            param.columns.push(column.column);
                        });
                        param.def.create = function() {
                            var table = new mssql.Table(param.def.typeName);
                            columns && columns.forEach(function(column) {
                                var type = mssql[column.type.toUpperCase()];
                                if (!(type instanceof Function)) {
                                    throw Error.create('Unexpected type ' + column.type + ' in stored procedure ' + binding.name);
                                }
                                table.columns.add(column.column, type(column.length, column.scale));
                            });
                            return table;
                        };
                    }
                });
                this.config[flatName] = this.callSP(binding.name, binding.params, flatten);
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
    request.multiple = true;
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
            user_name(objectproperty(o.object_id, 'OwnerId')) in (USER_NAME(),'dbo') AND
            objectproperty(o.object_id, 'IsMSShipped') = 0
        UNION ALL
        SELECT 0,0,0,'S',name,NULL,NULL,NULL FROM sys.schemas WHERE principal_id = USER_ID()
        UNION ALL
        SELECT
            0,0,0,'T',SCHEMA_NAME(t.schema_id)+'.'+t.name,NULL,NULL,NULL
        FROM
            sys.types t
        JOIN
            sys.schemas s ON s.principal_id = USER_ID() AND s.schema_id=t.schema_id
        WHERE
            t.is_user_defined=1
        ORDER BY
            1, 2, 3

        SELECT
            SCHEMA_NAME(types.schema_id) + '.' + types.name name,
            c.name [column],
            st.name type,
            CASE
                WHEN st.name in ('decimal','numeric') then c.[precision]
                WHEN st.name in ('varchar','nvarchar','nvarbinary') then c.max_length
            END [length],
            CASE
                WHEN st.name in ('decimal','numeric') then c.scale
            END scale
        FROM
            sys.table_types types
        JOIN
            sys.columns c ON types.type_table_object_id = c.object_id
        JOIN
            sys.systypes AS st ON st.xtype = c.system_type_id
        WHERE
            types.is_user_defined = 1
        ORDER BY
            1,c.column_id`;
    return request.query(sql).then(function(result) {
        var schema = {source: {}, parseList: [], types: {}};
        result[0].reduce(function(prev, cur) {
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
        }, schema);
        result[1].reduce(function(prev, cur) {
            if (!(mssql[cur.type.toUpperCase()] instanceof Function)) {
                throw Error.create('Unexpected column type ' + cur.type + ' in user defined table type ' + cur.name);
            }
            var type = prev[cur.name] || (prev[cur.name] = []);
            type.push(cur);
            return prev;
        }, schema.types);
        return schema;
    });
};

module.exports = SqlPort;
