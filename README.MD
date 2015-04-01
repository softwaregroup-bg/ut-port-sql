# **SQL Port:** `ut-port-sql` #
The purpose of this port is to work with SQL Server database. To be responsible for creating connections to the database and for query execution.

### **Technological dependencies** ###

 - `mssql` - [Github Official Page](https://github.com/patriksimek/node-mssql)
 - `ut-bus/port` - TODO add link to documentation

In the UT5 implementations the SQL port is initialized in the following manner:

	module.exports = {
	    id: 'sql',
	    type: 'sql',
	    logLevel: 'trace',
	    port: 8002,
	    db: {
	        user: 'switch',
	        password: 'switch',
	        server: 'DEVDB08',
	        database: 'UTNetSystem_3_2',
	        requestTimeout: 30000
	    },
	    receive: function(msg) {
	        return msg;
	    },
	    send: function(msg) {
	        return msg;
	    }
	};

and it is started in the implementation `server.js` like this:

    module.exports = {
        ports: [
            require('ports/sql')
        ],
        modules: [
            template: require('ut-template')
        ]
    }

The `start` method of this port, that is called from the `ut-bus`, initializes the SQL connection to the server and starts receiving requests once they come inside the in-going conversion. This port is synchronous and the requests are piped in a queue. 

Let's consider that we want to send `sql.CredentialsLogin` message to the SQL port's in-coming conversion. The message should look like this:

	msg = {
		$$: {
			mtid: 'request',
			opcode: 'sql.CredentialsLogin'
		},
		process: 'return', 
		query: '<THE SQL THAT HAS TO BE RUN>'
	}

The `$$` part is automatic, it's enough to specify `sql.CredentialsLogin` as namespace and opcode. In the out-going conversion you have to set the `process` and `query` parameters and in the in-going conversion you should receive the SQL response data.

Consider the following demo implementation of the SQL port, which uses `ut-template` to render the SQL queries from the file system and place them in the `query` parameter of the message that is to be send to the SQL server.

	module.exports = {
	    // ...
	    receive: function(msg) {
	        return msg;
	    },
	    send: function(msg) {
	        msg.process = 'json';
	        var template = this.bus.importMethod('template.load')('./templates/utNet_' + msg.$$.opcode + '.sql');
            return template.render(msg).then(function(res) {
                msg.query = res;
                return msg;
            });
	    }
	};

In this particular scenario we set `process` as JSON, which means that the response from the in-going conversion would have a data set of values. The other possible `process` values are return, xml ,csv, xsl.
From the example above we see that we load the SQL from a template directory within the sql port of the current implementation:
		
	/ports/sql/templates/utNet_CredentialsLogin.sql

The SQL port has a special method `schemaUpdate`, which updates the stored procedures for a particular implementation. It matches procedures from the file systems with those in the database and updates if differences are detected.