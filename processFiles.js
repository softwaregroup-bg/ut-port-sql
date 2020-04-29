const AUDIT_LOG = /^[\s+]{0,}--ut-audit-params$/m;
const CORE_ERROR = /^[\s+]{0,}EXEC \[?core]?\.\[?error]?$/m;
const CALL_PARAMS = /^[\s+]{0,}DECLARE @callParams XML$/m;
const PERMISSION_CHECK = /^[\s+]{0,}--ut-permission-check$/m;
const mssqlQueries = require('./sql');
const ENCRYPT_RE = /(?:NULL|0x.*)\/\*encrypt (.*)\*\//gi;
const ENCRYPTSTABLE_RE = /(?:NULL|0x.*)\/\*encryptStable (.*)\*\//gi;
const ROW_VERSION_INNER_TYPE = 'BINARY';
const VAR_RE = /\$\{([^}]*)\}/g;
const path = require('path');
const dotProp = require('dot-prop');
const parserSP = require('./parsers/mssqlSP');
const includes = require('ut-function.includes');
const yaml = require('yaml');
const fs = require('fs');

function changeRowVersionType(field) {
    if (field && (field.type.toUpperCase() === 'ROWVERSION' || field.type.toUpperCase() === 'TIMESTAMP')) {
        field.type = ROW_VERSION_INNER_TYPE;
        field.length = 8;
    }
}

function replaceAuditLog(binding, statement) {
    return statement.trim().replace(AUDIT_LOG, mssqlQueries.auditLog(binding));
}

function replaceCallParams(binding, statement) {
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

const preProcess = (binding, statement, fileName, objectName, cbc) => {
    if (cbc) {
        statement = statement.replace(ENCRYPT_RE, (match, value) => '0x' + cbc.encrypt(value).toString('hex'));
        statement = statement.replace(ENCRYPTSTABLE_RE, (match, value) => '0x' + cbc.encrypt(value, true).toString('hex'));
    }

    if (statement.match(AUDIT_LOG)) {
        statement = replaceAuditLog(binding, statement);
    }
    let params = 'NULL';
    if (statement.match(CALL_PARAMS)) {
        statement = replaceCallParams(binding, statement);
        params = '@callParams';
    }
    if (statement.match(CORE_ERROR)) {
        statement = replaceCoreError(statement, fileName, objectName, params);
    }
    return statement;
};

function getAlterStatement(binding, statement, fileName, objectName) {
    statement = preProcess(binding, statement, fileName, objectName);
    if (statement.trim().match(/^CREATE\s+TYPE/i)) {
        return statement.trim();
    } else {
        return statement.trim().replace(/^CREATE /i, 'ALTER ');
    }
}

function tableToType(binding) {
    if (!binding || binding.type !== 'table') return '';
    const name = binding.name.match(/]$/) ? binding.name.slice(0, -1) + 'TT]' : binding.name + 'TT';
    const columns = binding.fields.map(function(field) {
        changeRowVersionType(field);
        return `[${field.column}] [${field.type}]` +
            (field.length !== null && field.scale !== null ? `(${field.length},${field.scale})` : '') +
            (field.length !== null && field.scale === null ? `(${field.length})` : '') +
            (typeof field.default === 'number' ? ` DEFAULT(${field.default})` : '') +
            (typeof field.default === 'string' ? ` DEFAULT('${field.default.replace(/'/g, '\'\'')}')` : '');
    });
    return 'CREATE TYPE ' + name + ' AS TABLE (\r\n  ' + columns.join(',\r\n  ') + '\r\n)';
}

function tableToTTU(binding) {
    if (!binding || binding.type !== 'table') return '';
    const name = binding.name.match(/]$/) ? binding.name.slice(0, -1) + 'TTU]' : binding.name + 'TTU';
    const columns = binding.fields.map(function(field) {
        changeRowVersionType(field);
        return ('[' + field.column + '] [' + field.type + ']' +
            (field.length !== null && field.scale !== null ? `(${field.length},${field.scale})` : '') +
            (field.length !== null && field.scale === null ? `(${field.length})` : '') +
            ',\r\n' + field.column + 'Updated bit');
    });
    return 'CREATE TYPE ' + name + ' AS TABLE (\r\n  ' + columns.join(',\r\n  ') + '\r\n)';
}

function getCreateStatement(binding, statement, fileName, objectName, cbc) {
    return preProcess(binding, statement, fileName, objectName, cbc).trim()
        .replace(/^ALTER /i, 'CREATE ')
        .replace(/^DROP SYNONYM .* CREATE SYNONYM/i, 'CREATE SYNONYM');
}

function fieldSource(column) {
    return (column.column + '\t' + column.type + '\t' + (column.length === null ? '' : column.length) + '\t' + (column.scale === null ? '' : column.scale)).toLowerCase();
}

function getSource(binding, statement, fileName, objectName) {
    statement = preProcess(binding, statement, fileName, objectName);
    if (statement.trim().match(/^CREATE\s+TYPE/i)) {
        if (binding && binding.type === 'table type') {
            return binding.fields.map(fieldSource).join('\r\n');
        }
    }
    return statement.trim().replace(/^ALTER /i, 'CREATE ');
}

function addQuery(schema, queries, params) {
    if (schema.source[params.objectId] === undefined) {
        queries.push({fileName: params.fileName, objectName: params.objectName, objectId: params.objectId, content: params.createStatement});
    } else {
        if (schema.source[params.objectId].length &&
            (getSource(params.binding, params.fileContent, params.fileName, params.objectName) !== schema.source[params.objectId])) {
            const deps = schema.deps[params.objectId];
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
                content: getAlterStatement(params.binding, params.fileContent, params.fileName, params.objectName)
            });
        }
    }
}

function getObjectName(fileName) {
    return fileName.replace(/\.(sql|js|json|yaml)$/i, '').replace(/^[^$-]*[$-]/, ''); // remove "prefix[$-]" and ".sql/.js/.json" suffix
}

function shouldCreateTT(schemaConfig, tableName) {
    return (includes(schemaConfig.createTT, tableName)) && !includes(schemaConfig.skipTableType, tableName);
}

function interpolate(txt, params = {}) {
    return txt.replace(VAR_RE, (placeHolder, label) => {
        const value = dotProp.get(params, label);
        switch (typeof value) {
            case 'undefined': return placeHolder;
            case 'object': return JSON.stringify(value);
            default: return value;
        }
    });
};

const addSP = (queries, {fileName, objectName, objectId, config}) => {
    const params = path.extname(fileName).toLowerCase() === '.yaml'
        ? yaml.parse(fs.readFileSync(fileName, 'utf8'))
        : require(fileName);
    queries.push({
        fileName,
        objectName,
        objectId,
        callSP: function callSPFromJson() {
            if (typeof this.methods[objectName] !== 'function') {
                throw this.errors['portSQL.spNotFound']({params: {name: objectName}});
            }

            return this.methods[objectName].call(
                this,
                typeof params === 'function' ? params(config) : params,
                {
                    auth: {
                        actorId: 0
                    },
                    method: objectName,
                    userName: 'SYSTEM'
                }
            );
        }
    });
};

function processFiles(schema, busConfig, schemaConfig, files, vfs, cbc) {
    files = files.sort().map(file => {
        return {
            originalName: file,
            name: interpolate(file, busConfig)
        };
    });
    if (schemaConfig.exclude && schemaConfig.exclude.length > 0) {
        files = files.filter((file) => !(schemaConfig.exclude.indexOf(file.name) >= 0));
    }
    const objectIds = files.reduce(function(prev, cur) {
        prev[getObjectName(cur.name).toLowerCase()] = true;
        return prev;
    }, {});
    const queries = [];
    const dbObjects = {};
    files.forEach(function(file) {
        const objectName = getObjectName(file.name);
        const objectId = objectName.toLowerCase();
        const fileName = path.join(schemaConfig.path, file.originalName);
        try {
            if (!vfs.isFile(fileName)) {
                return;
            }
            switch (path.extname(fileName).toLowerCase()) {
                case '.sql': {
                    schemaConfig.linkSP && (dbObjects[objectId] = fileName);
                    let fileContent = interpolate(vfs.readFileSync(fileName).toString(), busConfig);
                    const binding = fileContent.trim().match(/^(\bCREATE\b|\bALTER\b)\s+(PROCEDURE|TABLE|TYPE)/i) && parserSP.parse(fileContent);
                    if (binding && binding.type === 'procedure' && includes(schemaConfig.permissionCheck, [objectId])) {
                        fileContent = fileContent.replace(PERMISSION_CHECK, mssqlQueries.permissionCheck());
                    }
                    addQuery(schema, queries, {
                        binding,
                        fileName,
                        objectName,
                        objectId,
                        fileContent,
                        createStatement: getCreateStatement(binding, fileContent, fileName, objectName, cbc)
                    });
                    if (shouldCreateTT(schemaConfig, objectId) && !objectIds[objectId + 'tt']) {
                        const tt = tableToType(binding);
                        if (tt) {
                            addQuery(schema, queries, {
                                binding: parserSP.parse(tt),
                                fileName,
                                objectName: objectName + 'TT',
                                objectId: objectId + 'tt',
                                fileContent: tt,
                                createStatement: tt
                            });
                        }
                        const ttu = tableToTTU(binding);
                        if (ttu) {
                            addQuery(schema, queries, {
                                binding: parserSP.parse(ttu),
                                fileName,
                                objectName: objectName + 'TTU',
                                objectId: objectId + 'ttu',
                                fileContent: ttu,
                                createStatement: ttu
                            });
                        }
                    };
                    if (binding && binding.type === 'table' && binding.options && binding.options.ngram) {
                        const [namespace, table] = objectName.split('.');
                        const ngramTT = mssqlQueries.ngramTT(namespace, table);
                        addQuery(schema, queries, {
                            binding: parserSP.parse(ngramTT),
                            fileName,
                            objectName: namespace + '.ngramTT',
                            objectId: namespace + '.ngramtt',
                            fileContent: ngramTT,
                            createStatement: ngramTT
                        });
                        schema.source[namespace + '.ngramtt'] = true;
                        if (binding.options.ngram.index) {
                            const ngramIndex = mssqlQueries.ngramIndex(namespace, table);
                            addQuery(schema, queries, {
                                binding: parserSP.parse(ngramIndex),
                                fileName,
                                objectName: objectName + 'Index',
                                objectId: objectId + 'index',
                                fileContent: ngramIndex,
                                createStatement: ngramIndex
                            });
                            const ngramIndexById = mssqlQueries.ngramIndexById(namespace, table);
                            addQuery(schema, queries, {
                                fileName,
                                objectName: 'ix' + objectName + 'IndexById',
                                objectId: 'ix' + objectId + 'indexbyid',
                                fileContent: ngramIndexById,
                                createStatement: ngramIndexById
                            });
                            const ngramIndexTT = mssqlQueries.ngramIndexTT(namespace);
                            addQuery(schema, queries, {
                                binding: parserSP.parse(ngramIndexTT),
                                fileName,
                                objectName: namespace + '.ngramIndexTT',
                                objectId: namespace + '.ngramindextt',
                                fileContent: ngramIndexTT,
                                createStatement: ngramIndexTT
                            });
                            schema.source[namespace + '.ngramindextt'] = true;
                            const ngramMerge = mssqlQueries.ngramMerge(namespace, table);
                            addQuery(schema, queries, {
                                fileName,
                                objectName: objectName + 'IndexMerge',
                                objectId: objectId + 'indexmerge',
                                fileContent: ngramMerge,
                                createStatement: ngramMerge
                            });
                        }
                        if (binding.options.ngram.search) {
                            const ngramSearch = mssqlQueries.ngramSearch(namespace, table);
                            addQuery(schema, queries, {
                                fileName,
                                objectName: objectName + 'Search',
                                objectId: objectId + 'search',
                                fileContent: ngramSearch,
                                createStatement: ngramSearch
                            });
                        }
                    }
                    break;
                }
                case '.js':
                case '.json':
                case '.yaml':
                    addSP(queries, {
                        fileName,
                        objectName: objectName,
                        objectId: objectId,
                        config: schemaConfig.config
                    });
                    break;
                default:
                    throw new Error('Unsupported file extension');
            };
        } catch (error) {
            error.message = error.message +
                ' (' +
                fileName +
                (error.location ? `:${error.location.start.line}:${error.location.start.column}` : '') +
                ')';
            throw error;
        }
    });
    return {queries, dbObjects};
};

module.exports = processFiles;
