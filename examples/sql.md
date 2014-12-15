        var SQL = require('ut-port-sql');

        var sql = new SQL(
            {
                user: 'switch',
                password: 'switch',
                server: '192.168.133.40',
                database: 'G_MFSP3_2',
            },
            require('ut-validate').get('joi').validateSql
        );

        sql.exec({
            _sql: {
                process: 'return',
                sql: 'select * from Account'
            },
            what: 'v1',
            ever: 'v2'
        }).then(function(msg) {
            console.log(msg);
        }).catch(function(err) {
           console.log(msg);
        });