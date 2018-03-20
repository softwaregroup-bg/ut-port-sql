var Transform = require('./transform');
let { getResultSetName } = require('./helpers');
class JsonTransform extends Transform {
    constructor(stream, config) {
        super(stream, config);
        this.options = {
            comma: '',
            resultsetPrev: null
        };
    }
    onStart() {
        this.stream.push(this.config.namedSet ? '{' : '[');
    }
    onEnd() {
        let { namedSet, single } = this.config;
        if (namedSet !== undefined) {
            if (single === false) {
                this.stream.push(']'); // push end of array literal If the last object is not a single
            }
            this.stream.push(namedSet ? '}' : ']'); //  write object end literal
        }
    }
    onRow(chunk) {
        let { comma } = this.options;
        this.stream.push(comma + JSON.stringify(chunk));
        if (comma === '') {
            this.options.comma = ',';
        }
    }
    onResultSet(chunk) {
        var current = getResultSetName(chunk);
        let { single } = this.config;
        let { comma, resultsetPrev } = this.options;
        if (resultsetPrev) {
            if (comma === '') {
                this.stream.push(resultsetPrev.single ? '{},' : '],'); // handling empty result set
            } else {
                this.stream.push(resultsetPrev.single ? ',' : '],'); // handling end
            }
        }
        this.stream.push(`"${current}": ${!single ? '[' : ''}`);
        this.options.comma = '';
        this.options.resultsetPrev =  chunk;
    }
}
module.exports = JsonTransform;
