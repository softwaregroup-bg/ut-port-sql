class Transform {
    constructor(stream, config) {
        this.stream = stream;
        this.config = config || {};
    }

    setOptions(key, value) {
        this.config[key] = value;
    }

    onStart() {};
    onRow(chunk) {}
    onResultSet(chunk) {}
    onEnd() {}
}

module.exports = Transform;
