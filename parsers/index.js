module.exports = driver => driver === 'oracle' ? require('./oracleSP') : require('./mssqlSP');
