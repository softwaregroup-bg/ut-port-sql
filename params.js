const ROW_VERSION_INNER_TYPE = 'BINARY';
const xml2js = require('xml2js');
const mssql = require('mssql');
const lodashGet = require('lodash.get');
const xmlBuilder = new xml2js.Builder({headless: true});
const isEncrypted = item => item && ((item.def && item.def.type === 'varbinary' && item.def.size % 16 === 0) || (item.length % 16 === 0) || /^encrypted/.test(item.name));
const ngram = require('ut-function.ngram');
const TAGS = /(\p{Letter}|\d|=|\.)+/gu;

function addNgram(hmac, ngramParam, add, row, param, column, string) {
    const options = ngramParam.options && ngramParam.options[column];
    if (!options) return ngramParam;
    const id = typeof options === 'number' ? options : options.id;
    const tags = options.tags || column?.endsWith?.('Tags');
    const unique = tags
        ? new Set(string.toLowerCase().match(TAGS))
        : ngram(string.toLowerCase(), options);

    unique.forEach(ngram => add(row, id || 0, param, hmac((options.name ? options.name : column) + ' ' + ngram)));
}

function getValue(cbc, hmac, ngram, index, param, column, value, def, updated) {
    const calcNgram = what => what && ngram && addNgram(hmac, ngram, ngram.add, index, param.name, param.name + '.' + column.name, what);
    if (updated) {
        return updated;
    }
    if (value === undefined) {
        calcNgram(def);
        return def;
    } else if (value != null) {
        if (cbc && isEncrypted({name: column.name, def: {type: column.type.declaration, size: column.length}})) {
            if (value === '') return Buffer.alloc(0);
            if (!Buffer.isBuffer(value) && !(value instanceof Date) && typeof value === 'object') value = JSON.stringify(value);
            calcNgram(value);
            return cbc.encrypt(value, column.name);
        } else if (/^(date.*|smalldate.*)$/.test(column.type.declaration)) {
            // set a javascript date for 'date', 'datetime', 'datetime2' 'smalldatetime'
            return new Date(value);
        } else if (typeof value === 'string' && column.type.declaration.toUpperCase() === ROW_VERSION_INNER_TYPE) {
            return /^[0-9A-Fa-f]+$/.test(value) ? Buffer.from(value, 'hex') : Buffer.from(value, 'utf-8');
        } else if (column.type.declaration === 'time') {
            return value?.includes?.('T') ? new Date(value) : new Date('1970-01-01T' + value + 'Z');
        } else if (column.type.declaration === 'xml') {
            const obj = {};
            obj[column.name] = value;
            return xmlBuilder.buildObject(obj);
        } else if (value.type === 'Buffer') {
            return Buffer.from(value.data);
        } else if (typeof value === 'object' && !(value instanceof Date) && !Buffer.isBuffer(value)) {
            value = JSON.stringify(value);
            return column.type.declaration === 'varbinary' ? Buffer.from(value) : value;
        } else if (
            value != null &&
            typeof value !== 'string' &&
            value.toString &&
            column.type &&
            ['char', 'nchar', 'varchar', 'nvarchar', 'text', 'ntext', 'uniqueidentifier'].includes(column.type.declaration)
        ) {
            value = value.toString();
        }
    }
    calcNgram(value);
    return value;
}

function sqlType(def, driver) {
    if (driver === 'oracle') return def;
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

function setParam(cbc, hmac, ngram, request, param, value, driver) {
    if (param.encrypt && value != null) {
        if (!Buffer.isBuffer(value) && !(value instanceof Date) && typeof value === 'object') value = JSON.stringify(value);
        ngram && addNgram(hmac, ngram, ngram.add, 1, param.name, param.name, value);
        value = cbc.encrypt(value, param.name);
    }
    const hasValue = value !== undefined;
    const type = sqlType(param.def, driver);
    if (param.def && param.def.type === 'time' && value != null) {
        value = value?.includes?.('T') ? value : new Date('1970-01-01T' + value);
    } else if (param.def && /datetime/.test(param.def.type) && value != null && !(value instanceof Date)) {
        value = new Date(value);
    } else if (param.def && param.def.type === 'xml' && value != null) {
        value = xmlBuilder.buildObject(value);
    } else if (param.def && param.def.type === 'rowversion' && value != null && !Buffer.isBuffer(value)) {
        value = Buffer.from(value.data ? value.data : []);
    } else if (value != null && typeof value === 'object' && !(value instanceof Date) && !Buffer.isBuffer(value) && (!param.def || !['table', 'nested'].includes(param.def.type))) {
        value = JSON.stringify(value);
        if (param.def.type === 'varbinary') value = Buffer.from(value);
    } else if (
        value != null &&
        typeof value !== 'string' &&
        value.toString &&
        param.def &&
        ['char', 'nchar', 'varchar', 'nvarchar', 'text', 'ntext', 'uniqueidentifier'].includes(param.def.type)
    ) {
        value = value.toString();
    }
    if (param.out) {
        request.output(param.name, type, value);
    } else {
        if (param.def && param.def.type === 'table') {
            if (value) {
                if (Array.isArray(value)) {
                    value.forEach(function(row, rowIndex) {
                        if (param.def.typeName && param.def.typeName.endsWith('.ngramTT')) {
                            addNgram(hmac, param, (...columns) => type.rows.add(...columns), 1, 'search', row[0], row[1]);
                            return;
                        }
                        if (typeof row === 'object') {
                            type.rows.add(...param.columns.map((column, index) => getValue(
                                cbc,
                                hmac,
                                ngram,
                                rowIndex + 1,
                                param,
                                type.columns[index],
                                param.flatten ? lodashGet(row, column.column.split(param.flatten)) : row[column.column],
                                column.default,
                                column.update && Object.prototype.hasOwnProperty.call(row, column.update)
                            )));
                        } else {
                            type.rows.add(getValue(
                                cbc,
                                hmac,
                                ngram,
                                rowIndex + 1,
                                param,
                                type.columns[0],
                                row,
                                param.columns[0].default,
                                false
                            ), ...new Array(param.columns.length - 1));
                        }
                    });
                } else if (typeof value === 'object') {
                    type.rows.add(...param.columns.map((column, index) => getValue(
                        cbc,
                        hmac,
                        ngram,
                        1,
                        param,
                        type.columns[index],
                        param.flatten ? lodashGet(value, column.column.split(param.flatten)) : value[column.column],
                        column.default,
                        column.update && Object.prototype.hasOwnProperty.call(value, column.update)
                    )));
                } else {
                    type.rows.add(getValue(
                        cbc,
                        hmac,
                        ngram,
                        1,
                        param,
                        type.columns[0],
                        value,
                        param.columns[0].default,
                        false
                    ), ...new Array(param.columns.length - 1));
                }
            }
            request.input(param.name, type);
        } else {
            if (!param.default || hasValue) {
                request.input(param.name, type, value);
            }
        }
    }
    return value;
}

module.exports = {setParam, isEncrypted};
