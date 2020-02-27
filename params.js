const ROW_VERSION_INNER_TYPE = 'BINARY';
const xml2js = require('xml2js');
const mssql = require('mssql');
const xmlBuilder = new xml2js.Builder({headless: true});
const isEncrypted = item => item && ((item.def && item.def.type === 'varbinary' && item.def.size % 16 === 0) || (item.length % 16 === 0) || /^encrypted/.test(item.name));
const WORDS = /(\p{Letter}|\d)+/gu;
const LETTER = /\p{Letter}|\d/gu;

function addNgram(hmac, ngramParam, add, row, param, column, string) {
    const options = ngramParam.options && ngramParam.options[column];
    if (!options) return ngramParam;
    const id = typeof options === 'number' ? options : options.id;
    const unique = new Set();
    string.toLowerCase().match(WORDS).forEach(word => {
        word = word.match(LETTER);
        if (word) {
            const {min = 3, max = 3, depth = word.length - min} = options;
            const length = word.length;
            if (min >= length) {
                unique.add(word.join(''));
            } else {
                for (let i = 0; i <= depth && i <= length; i++) {
                    for (let j = min; j <= max && i + j <= length; j++) {
                        unique.add(word.slice(i, i + j).join(''));
                    }
                }
            }
        }
    });
    unique.forEach(ngram => add(row, id || 0, param, hmac(column + ' ' + ngram)));
}

function getValue(cbc, hmac, ngram, index, param, column, value, def, updated) {
    if (updated) {
        return updated;
    }
    if (value === undefined) {
        return def;
    } else if (value) {
        if (cbc && isEncrypted({name: column.name, def: {type: column.type.declaration, size: column.length}})) {
            if (!Buffer.isBuffer(value) && typeof value === 'object') value = JSON.stringify(value);
            ngram && addNgram(hmac, ngram, ngram.add, index, param.name, param.name + '.' + column.name, value);
            return cbc.encrypt(Buffer.from(value));
        } else if (/^(date.*|smalldate.*)$/.test(column.type.declaration)) {
            // set a javascript date for 'date', 'datetime', 'datetime2' 'smalldatetime'
            return new Date(value);
        } else if (typeof value === 'string' && column.type.declaration.toUpperCase() === ROW_VERSION_INNER_TYPE) {
            return /^[0-9A-Fa-f]+$/.test(value) ? Buffer.from(value, 'hex') : Buffer.from(value, 'utf-8');
        } else if (column.type.declaration === 'time') {
            return new Date('1970-01-01T' + value + 'Z');
        } else if (column.type.declaration === 'xml') {
            const obj = {};
            obj[column.name] = value;
            return xmlBuilder.buildObject(obj);
        } else if (value.type === 'Buffer') {
            return Buffer.from(value.data);
        } else if (typeof value === 'object' && !(value instanceof Date)) {
            return JSON.stringify(value);
        }
    }
    return value;
}

function flattenMessage(data, delimiter, limit) {
    if (!delimiter) {
        return data;
    }
    const result = {};
    function flatten(cur, prop, depth) {
        if (depth > limit) throw new Error('Unsupported deep nesting for property ' + prop);
        if (typeof cur === 'function') {
        } else if (Object(cur) !== cur) {
            result[prop] = cur;
        } else if (Array.isArray(cur)) {
            // for (let i = 0, l = cur.length; i < l; i += 1) {
            //     flat(cur[i], prop + '[' + i + ']');
            // }
            // if (l === 0) {
            //     result[prop] = [];
            // }
            result[prop] = cur;
        } else {
            let isEmpty = true;
            for (const p in cur) {
                isEmpty = false;
                flatten(cur[p], prop ? prop + delimiter + p : p, depth + 1);
            }
            if (isEmpty && prop) {
                result[prop] = {};
            }
        }
    }
    flatten(data, '', 1);
    return result;
}

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

function setParam(cbc, hmac, ngram, request, param, value, limit) {
    if (param.encrypt && value != null) {
        ngram && addNgram(hmac, ngram, ngram.add, 1, param.name, param.name, value);
        value = cbc.encrypt(Buffer.from(value));
    }
    const hasValue = value !== undefined;
    const type = sqlType(param.def);
    if (param.def && param.def.type === 'time' && value != null) {
        value = new Date('1970-01-01T' + value);
    } else if (param.def && /datetime/.test(param.def.type) && value != null && !(value instanceof Date)) {
        value = new Date(value);
    } else if (param.def && param.def.type === 'xml' && value != null) {
        value = xmlBuilder.buildObject(value);
    } else if (param.def && param.def.type === 'rowversion' && value != null && !Buffer.isBuffer(value)) {
        value = Buffer.from(value.data ? value.data : []);
    } else if (value != null && typeof value === 'object' && !(value instanceof Date) && !Buffer.isBuffer(value) && (!param.def || param.def.type !== 'table')) {
        value = JSON.stringify(value);
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
                        row = flattenMessage(row, param.flatten, limit);
                        if (typeof row === 'object') {
                            type.rows.add(...param.columns.map((column, index) => getValue(
                                cbc,
                                hmac,
                                ngram,
                                rowIndex + 1,
                                param,
                                type.columns[index],
                                row[column.column],
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
                    value = flattenMessage(value, param.flatten, limit);
                    type.rows.add(...param.columns.map((column, index) => getValue(
                        cbc,
                        hmac,
                        ngram,
                        1,
                        param,
                        type.columns[index],
                        value[column.column],
                        column.default,
                        column.update && Object.prototype.hasOwnProperty.call(value, column.update)
                    )));
                } else {
                    value = flattenMessage(value, param.flatten, limit);
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

module.exports = {setParam, isEncrypted, flattenMessage};
