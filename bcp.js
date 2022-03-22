const exec = require('child_process').spawnSync;
const fs = require('fs');
const os = require('os');
const path = require('path');
const uuid = require('uuid');

module.exports = function bcp({
    table,
    command,
    file,
    user,
    password,
    server,
    port,
    trustServerCertificate,
    database,
    separator,
    terminator,
    firstRow = 1,
    lastRow,
    maxErrors = 0,
    hints,
    formatFile,
    tempDir = os.tmpdir(),
    unicode = true
}) {
    if (!table) throw new Error('bcp: Missing table parameter');
    if (!['in', 'out', 'queryout', 'format'].includes(command)) throw new Error('bcp: Invalid command');
    if (!tempDir) throw new Error('bcp: Unknown os.tmpdir()');
    const tempFileName = path.join(tempDir, uuid.v4() + '-bcp-errors.txt');
    const execResult = exec('bcp', [
        table,
        command,
        command === 'format' ? 'nul' : file,
        command === 'format' && (unicode ? '-w' : '-c'),
        `-e${tempFileName}`,
        formatFile && `-f${formatFile}`,
        maxErrors && `-m${maxErrors}`,
        firstRow && `-F${firstRow}`,
        lastRow && `-L${lastRow}`,
        hints && `-h${hints}`,
        separator && `-t${separator}`,
        terminator && `-r${terminator}`,
        trustServerCertificate && '-u',
        user && `-U${user}`,
        database && `-d${database}`,
        server && (port ? `-S${server},${port}` : `-S${server}`)
    ].filter(Boolean), {
        input: password + '\n'
    });
    let errors = fs.existsSync(tempFileName);
    let output;
    try {
        if (execResult.error) throw execResult.error;
        output = execResult.stdout.toString();
        if (execResult.status !== 0) throw new Error(output.replace(/^Password:.*\r?\n/, ''));
        errors = fs.readFileSync(tempFileName).toString();
    } finally {
        if (errors) fs.unlinkSync(tempFileName);
    }
    const rows = output.match(/^(\d+) rows copied\./m);
    return {
        rows: rows ? parseInt(rows[1], 10) : undefined,
        errors
    };
};
