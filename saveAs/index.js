const formats = {
    jsonl: require('./jsonl')
};

module.exports = (request, saveAs = {}) => {
    const config = typeof saveAs === 'string' ? {filename: saveAs} : saveAs;
    const ext = config.filename.split('.').pop();
    const Format = formats[ext] || formats.jsonl;
    const format = new Format(request, config);
    return format.stream;
};
