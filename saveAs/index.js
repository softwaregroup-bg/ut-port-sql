const formats = {
    jsonl: require('./jsonl'),
    json: require('./json'),
    csv: require('./csv')
};

module.exports = (port, request, filename = {}) => {
    const config = typeof filename === 'string' ? {filename} : filename;
    const ext = config.filename.split('.').pop();
    const Format = formats[ext] || formats.jsonl;
    const format = new Format(port, request, config);
    return format;
};
