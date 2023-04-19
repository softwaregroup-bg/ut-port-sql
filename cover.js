const libCoverage = require('istanbul-lib-coverage');
const {writeFileSync} = require('fs');
const {relative, resolve} = require('path');

module.exports = function(coverage) {
    const coverageMap = libCoverage.createCoverageMap();

    Object.entries(coverage).forEach(([path, counts]) => {
        const statements = Object.entries(counts);
        coverageMap.addFileCoverage(libCoverage.createFileCoverage({
            path,
            statementMap: Object.fromEntries(statements.map(([statement], index) => {
                const [startLine, startColumn, endLine, endColumn] = statement.split(' ').map(Number);
                return [index, {
                    start: {
                        line: startLine,
                        column: startColumn - 1
                    },
                    end: {
                        line: endLine,
                        column: endColumn - 1
                    }
                }];
            })),
            s: Object.fromEntries(statements.map(([, count], index) => [index, count])),
            fnMap: {},
            branchMap: {},
            f: {},
            b: {}
        }));
    });

    writeFileSync(
        resolve(`.nyc_output/${relative('.', require.main.filename).trim().replace(/[^a-zA-Z0-9._-]+/g, '-')}.sql.json`),
        JSON.stringify(coverageMap.toJSON())
    );
};
