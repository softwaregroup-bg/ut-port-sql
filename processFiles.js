const AUDIT_LOG = /^[\s+]{0,}--ut-audit-params$/m;
const CORE_ERROR = /^[\s+]{0,}(RETURN)? EXEC \[?core]?\.\[?error]?(?:[\s+]{0,}(@type = .*))?(?:$|;)/mi;
const CALL_PARAMS = /^[\s+]{0,}DECLARE @callParams XML($|;.*$)/m;
const PERMISSION_CHECK = /--ut-permission-check(.*)$/gm;
const mssqlQueries = require('./sql');
const ENCRYPT_RE = /(?:NULL|0x.*)\/\*encrypt (.*)\*\//gi;
const ENCRYPTSTABLE_RE = /(?:NULL|0x.*)\/\*encryptStable (.*)\*\//gi;
const ROW_VERSION_INNER_TYPE = 'BINARY';
const VAR_RE = /\$\{([^}]*)\}/g;
const SECTION_RE = /--\{([^}\s]+)[^}]*\}/g;
const path = require('path');
const get = require('lodash.get');
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
    return statement.trim().replace(CALL_PARAMS, mssqlQueries.callParams(binding) + '$1');
}

function replaceCoreError(statement, fileName, objectName, params) {
    return statement
        .split('\n')
        .map((line, index) => (line.replace(CORE_ERROR, (match, ret, type) =>
            `DECLARE @CORE_ERROR_FILE_${index} SYSNAME='${fileName.replace(/'/g, '\'\'')}' ` +
            `DECLARE @CORE_ERROR_LINE_${index} INT='${index + 1}' ` +
            `DECLARE @ERROR_NUMBER_LINE_${index} INT=ERROR_NUMBER()+100000 ` +
            `${ret || ''} EXEC [core].[errorStack] @procid=@@PROCID, @file=@CORE_ERROR_FILE_${index}, @fileLine=@CORE_ERROR_LINE_${index}, @params = ${params}${type ? `, ${type}` : `, @errorNumber = @ERROR_NUMBER_LINE_${index};`}`)))
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
    } else if (binding?.params?.find(param => param.name === 'callParams' && param.def.type === 'xml')) {
        params = '@callParams';
    }
    if (statement.match(CORE_ERROR)) {
        statement = replaceCoreError(statement, fileName, objectName, params);
    }
    return statement;
};

function getAlterStatement(binding, statement, fileName, objectName, cbc) {
    statement = preProcess(binding, statement, fileName, objectName, cbc);
    if (statement.trim().match(/^CREATE\s+TYPE|^CREATE\s+(\bOR\b\s+\bREPLACE\b\s+)/i)) {
        return statement.trim();
    } else {
        return statement.trim().replace(/^CREATE /i, 'ALTER ');
    }
}

const fieldDef = genNullable => field => {
    changeRowVersionType(field);
    return `[${field.column}] [${field.type}]` +
        (field.length !== null && field.scale !== null ? `(${field.length},${field.scale})` : '') +
        (field.length !== null && field.scale === null ? `(${field.length})` : '') +
        (genNullable && !field.nullable ? ' NOT NULL' : '') +
        (typeof field.default === 'number' ? ` DEFAULT(${field.default})` : '') +
        (typeof field.default === 'string' ? ` DEFAULT('${field.default.replace(/'/g, '\'\'')}')` : '');
};

function tableToType(binding, driver) {
    if (!binding || binding.type !== 'table') return '';
    if (driver === 'oracle') return `TYPE "${binding.table}TT" IS TABLE OF "${binding.schema}.${binding.table}"%ROWTYPE;`;
    const name = binding.name.match(/]$/) ? binding.name.slice(0, -1) + 'TT]' : binding.name + 'TT';
    const columns = binding.fields.map(fieldDef(false));
    return 'CREATE TYPE ' + name + ' AS TABLE (\r\n  ' + columns.join(',\r\n  ') + '\r\n)';
}

function tableToTTU(binding, driver) {
    if (!binding || binding.type !== 'table') return '';
    if (driver === 'oracle') return '';
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

function getSource(binding, statement, fileName, objectName, cbc) {
    statement = preProcess(binding, statement, fileName, objectName, cbc);
    if (statement.trim().match(/^CREATE\s+TYPE/i)) {
        if (binding && binding.type === 'table type') {
            return binding.fields.map(fieldSource).join('\r\n');
        }
    }
    return statement.trim().replace(/^ALTER /i, 'CREATE ');
}

function addQuery(schema, queries, params, cbc, driver) {
    if (schema.source[params.objectId] === undefined) {
        queries.push({
            binding: params.binding,
            fileName: params.fileName,
            objectName: params.objectName,
            objectId: params.objectId,
            content: params.createStatement
        });
    } else {
        if (schema.source[params.objectId].length &&
            (getSource(params.binding, params.fileContent, params.fileName, params.objectName, cbc) !== schema.source[params.objectId])) {
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
                binding: params.binding,
                fileName: params.fileName,
                objectName: params.objectName,
                objectId: params.objectId,
                content: getAlterStatement(params.binding, params.fileContent, params.fileName, params.objectName, cbc)
            });
        }
    }
    if (driver === 'oracle' && params?.binding?.type === 'table type') {
        const found = queries.findIndex(query => query?.binding?.type === 'table type' && params?.binding?.schema === query?.binding?.schema);
        const last = queries[queries.length - 1];
        if (found >= 0 && found < queries.length - 1) {
            last.content = queries[found].content.replace(/^ {2}--types$/m, '  ' + last.content + '\n  --types');
            queries.splice(found, 1);
        } else {
            last.content = `CREATE OR REPLACE PACKAGE "${params.binding.schema}"
AS
  ${last.content}
  --types
END;`;
        }
    }
}

function getObjectName(fileName) {
    return fileName.replace(/\.(plsql|sql|js|json|yaml|fmt)$/i, '').replace(/^[^$-]*[$-]/, '').replace(/\[|]/g, ''); // remove "prefix[$-]" and ".sql/.js/.json" suffix
}

function shouldCreateTT(schemaConfig, tableName) {
    return (includes(schemaConfig.createTT, tableName)) && !includes(schemaConfig.skipTableType, tableName);
}

function interpolate(txt, params = {}) {
    const replacer = (placeHolder, label) => {
        const value = get(params, label, placeHolder);
        switch (typeof value) {
            case 'object': return JSON.stringify(value);
            default: return value;
        }
    };
    return txt.replace(VAR_RE, replacer).replace(SECTION_RE, replacer);
}

const addSP = (queries, {fileName, objectName, objectId, config, createParams}) => {
    const params = path.extname(fileName).toLowerCase() === '.yaml'
        ? yaml.parse(interpolate(fs.readFileSync(fileName, 'utf8'), config))
        : require(fileName);
    queries.push({
        fileName,
        objectName,
        objectId,
        callSP: function callSPFromJson() {
            if (typeof this.methods[objectName] !== 'function') {
                if (path.basename(fileName).includes('[')) return Promise.resolve(false);
                throw this.errors['portSQL.spNotFound']({params: {name: objectName}});
            }

            return this.methods[objectName].call(
                this,
                typeof params === 'function' ? params(config, createParams) : params,
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

module.exports = createParams => ({
    processFiles(schema, busConfig, schemaConfig, files, cbc, driver, cover, alterTable) {
        const parserSP = require('./parsers')(driver);
        files = files.sort().map(file => {
            return {
                originalName: file,
                name: interpolate(file, busConfig)
            };
        });
        if (schemaConfig.exclude) {
            files = files.filter(file => !includes(schemaConfig.exclude, [file.originalName]));
        }
        if (schemaConfig.config && schemaConfig.config.exclude) {
            files = files.filter(file => !includes(schemaConfig.config.exclude, [file.originalName]));
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
                if (!createParams.vfs.isFile(fileName)) {
                    return;
                }
                switch (path.extname(fileName).toLowerCase()) {
                    case '.plsql':
                    case '.sql': {
                        includes(schemaConfig.linkSP, [objectId]) && (dbObjects[objectId] = fileName);
                        let fileContent = interpolate(createParams.vfs.readFileSync(fileName).toString(), schemaConfig.config);
                        fileContent = interpolate(fileContent, busConfig);
                        const binding = fileContent.trim().match(/^(\bCREATE\b|\bALTER\b)\s+(\bOR\b\s+\bREPLACE\b\s+)?(PROCEDURE|TABLE|TYPE)/i) && parserSP.parse(fileContent, fileName, {startRule: cover ? 'createBody' : 'create'});
                        if (cover && binding && binding.type === 'procedure') {
                            fileContent = binding.body;
                            cover[fileName] = Object.fromEntries(Array.from(fileContent.matchAll(/;PRINT\('ut-cover [^=]*=(\d+ \d+ \d+ \d+)'\);/g)).map(([, x]) => [x, 0]));
                        }
                        const {permissionCheck = true} = schemaConfig;
                        if (binding && binding.type === 'procedure' && includes(permissionCheck, [objectId])) {
                            fileContent = fileContent.replace(PERMISSION_CHECK, (match, p1, offset) => {
                                const params = {offset};
                                if (p1) {
                                    p1.split(',').forEach(part => {
                                        const [key, value] = part.split('=').map(str => str.trim());
                                        params[key] = value;
                                    });
                                }
                                return mssqlQueries.permissionCheck(params);
                            });
                        }
                        if (alterTable && binding && binding.type === 'table') {
                            let add = '';
                            const alter = [];
                            binding.fields.forEach(field => {
                                const table = schema.tables[binding.schema + '.' + binding.table];
                                if (table) {
                                    const existing = table[field.column.toLowerCase()];
                                    if (!existing) {
                                        // add missing column
                                        if (add) add = add + ',\n';
                                        add += fieldDef(true)(field);
                                    } else if (existing && existing.type === field.type && field.length && existing.length < field.length) {
                                        // alter smaller column
                                        alter.push(fieldDef(true)(field));
                                    }
                                }
                            });
                            let alterTable = '';
                            if (add) alterTable = `ALTER TABLE ${binding.name} ADD\n${add};\n`;
                            if (alter.length) alterTable += alter.map(col => `ALTER TABLE ${binding.name} ALTER COLUMN ${col};\n`).join('');
                            if (alterTable) {
                                addQuery(schema, queries, {
                                    fileName,
                                    objectName: objectName + 'Alter',
                                    objectId: objectId + 'alter',
                                    fileContent: alterTable,
                                    createStatement: alterTable
                                }, cbc);
                            }
                        }
                        addQuery(schema, queries, {
                            binding,
                            fileName,
                            objectName,
                            objectId,
                            fileContent,
                            createStatement: getCreateStatement(binding, fileContent, fileName, objectName, cbc)
                        }, cbc);
                        if (shouldCreateTT(schemaConfig, objectId) && !objectIds[objectId + 'tt']) {
                            const tt = tableToType(binding, driver);
                            if (tt) {
                                addQuery(schema, queries, {
                                    binding: parserSP.parse(tt),
                                    fileName,
                                    objectName: objectName + 'TT',
                                    objectId: objectId + 'tt',
                                    fileContent: tt,
                                    createStatement: tt
                                }, cbc, driver);
                            }
                            const ttu = tableToTTU(binding, driver);
                            if (ttu) {
                                addQuery(schema, queries, {
                                    binding: parserSP.parse(ttu),
                                    fileName,
                                    objectName: objectName + 'TTU',
                                    objectId: objectId + 'ttu',
                                    fileContent: ttu,
                                    createStatement: ttu
                                }, cbc, driver);
                            }
                        }
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
                            }, cbc);
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
                                }, cbc);
                                const ngramIndexById = mssqlQueries.ngramIndexById(namespace, table);
                                addQuery(schema, queries, {
                                    fileName,
                                    objectName: 'ix' + objectName + 'IndexById',
                                    objectId: 'ix' + objectId + 'indexbyid',
                                    fileContent: ngramIndexById,
                                    createStatement: ngramIndexById
                                }, cbc);
                                const ngramIndexTT = mssqlQueries.ngramIndexTT(namespace);
                                addQuery(schema, queries, {
                                    binding: parserSP.parse(ngramIndexTT),
                                    fileName,
                                    objectName: namespace + '.ngramIndexTT',
                                    objectId: namespace + '.ngramindextt',
                                    fileContent: ngramIndexTT,
                                    createStatement: ngramIndexTT
                                }, cbc);
                                schema.source[namespace + '.ngramindextt'] = true;
                                const ngramMerge = mssqlQueries.ngramMerge(namespace, table);
                                addQuery(schema, queries, {
                                    fileName,
                                    objectName: objectName + 'IndexMerge',
                                    objectId: objectId + 'indexmerge',
                                    fileContent: ngramMerge,
                                    createStatement: ngramMerge
                                }, cbc);
                            }
                            if (binding.options.ngram.search) {
                                const ngramSearch = mssqlQueries.ngramSearch(namespace, table);
                                addQuery(schema, queries, {
                                    fileName,
                                    objectName: objectName + 'Search',
                                    objectId: objectId + 'search',
                                    fileContent: ngramSearch,
                                    createStatement: ngramSearch
                                }, cbc);
                            }
                        }
                        break;
                    }
                    case '.js':
                    case '.json':
                    case '.yaml':
                        addSP(queries, {
                            fileName,
                            objectName,
                            objectId,
                            config: schemaConfig.config,
                            createParams
                        });
                        break;
                    default:
                        throw new Error('Unsupported file extension');
                }
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
    },
    getObjectName
});
