module.exports = driver => {
    const parser = driver === 'oracle' ? require('./oracleSP') : require('./mssqlSP');
    return {
        ...parser,
        parse(input, filename, options) {
            try {
                return parser.parse(input, {filename, ...options});
            } catch (error) {
                if (filename) error.message = `${filename}:${error.location?.start?.line}:${error.location?.start?.column}\n${error.message}`;
                throw error;
            }
        }
    };
};
