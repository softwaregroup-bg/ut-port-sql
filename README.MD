# **SQL Port:** `ut-port-sql` #
The purpose of this port is to work with SQL Server database. To be responsible for creating connections to the database and for query execution.

### **Dependencies** ###

 - `mssql` - [Github Official Page](https://github.com/patriksimek/node-mssql)

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
        process: 'return',
        query: '<THE SQL THAT HAS TO BE RUN>'
    }

The `$meta` part is automatic, it's enough to specify `sql.CredentialsLogin` as namespace and opcode. In the out-going conversion you have to set the `process` and `query` parameters and in the in-going conversion you should receive the SQL response data.

Consider the following demo implementation of the SQL port, which uses `ut-template` to render the SQL queries from the file system and place them in the `query` parameter of the message that is to be send to the SQL server.

    module.exports = {
        // ...
        receive: function(msg) {
            return msg;
        },
        send: function(msg, $meta) {
            msg.process = 'json';
            var template = this.bus.importMethod('template.load')('./templates/utNet_' + $meta.opcode + '.sql');
            return template.render(msg).then(function(res) {
                msg.query = res;
                return msg;
            });
        }
    };

In this particular scenario we set `process` as JSON, which means that the response from the in-going conversion would have a data set of values. The other possible `process` values are return, xml ,csv, xsl.
From the example above we see that we load the SQL from a template directory within the sql port of the current implementation:

    /ports/sql/templates/utNet_CredentialsLogin.sql


### **Schema sync** ###

The SQL port has a configuration property named 'schema'. This property can be set to an array of object, specifying folders that contain source code of database objects.
Here is example:

    ```js
    module.exports = {
        type: 'sql',
        namespace: ['s1','s2']
        schema: [
            {path:__dirname + '/schema/1', linkSP:true},
            {path:__dirname + '/schema/2', linkSP:false},
        ]
    }
    ```

This configuration will make the port scan the specified folders and synchronize database schema. In addition, procedures from the first folder will be automatically 
linked to the port, so that they can be executed directly, based on their schema name and procedure name. So for the following procedure:
    
  ```sql
  CREATE PROCEDURE schemaName.storedProcedureName
    @param1 int,
    @param2 nvarchar(20)
    @param3 tableType
  AS
    ...
  ```
    
The stored procedure can be called using this:

  ```js
  bus.importMethod('schemaName.storedProcedureName')({param1:1, param2:'value', param3:[{column1:'value row 1'},{column1:'value row 2'}]})
  ```

The schema sync algorithm matches database objects to files in the file system and updates the database objects if differences are detected.
The file names in the file system are supposed to follow the following naming: `xxx$schemaName.objectName.sql` , where xxx are numbers, that control the order of execution. The below convention is recommended. 
Note the **camelCase** naming convention. File names, including extension are **case sensitive**.

 - `150$shemaName.sql` for **schema** creation:

   ```sql
   CREATE SCHEMA [schemaName]
   ```

 - `250$schemaName.synonymName.sql` for **single synonym per file** creation. Following the below exact query format (i.e. using the [ and ] brackets and no extra spaces) and filename matches the `xxx$shema.object` pattern. Any deviation from the below query format will lead to pointless recreation of synonyms: 

   ```sql
   DROP SYNONYM [schemaName].[synonymName] CREATE SYNONYM [schemaName].[synonymName] FOR [foreignDatabaseName].[foreignSchemaName].[foreignObjectName]
   ```

 - `250$schemaName.scriptName.sql` for **multiple synonyms per file** creation. Filename should not match any object name in the database. The recommended queries in this case are:

   ```sql
   IF NOT EXISTS (
     SELECT 
       * 
     FROM 
       sys.synonyms sy 
     JOIN 
       sys.schemas s ON s.schema_id=sy.schema_id AND s.name='schemaName' 
     WHERE 
       sy.base_objectName='[foreignDatabaseName].[foreignSchemaName].[foreignObjectName]' AND
       sy.type='SN' and sy.name='synonymName'
   )
   BEGIN
     IF EXISTS (
     SELECT 
       * 
     FROM 
       sys.synonyms sy 
     JOIN 
       sys.schemas s ON s.schema_id=sy.schema_id AND s.name='schemaName' 
     WHERE 
       sy.type='SN' and sy.name='synonymName'
     ) DROP SYNONYM [schemaName].[synonymName]
     CREATE SYNONYM [schemaName].[synonymName] FOR [foreignDatabaseName].[foreignSchemaName].[foreignObjectName]
   END
   ```

 - `340$schemaName.typeName.sql` for **used defined table type**. 
 The script will be executed as is, only when the used defined table type does not exist. 
 Altering user defined types is currently related to the complexity of other objects depending on the type,
 so currently there is no automatic way for modifying already existing type. 

   ```sql
   CREATE TYPE [schemaName].[typeName] AS TABLE(
     column1 int,
     ... -- other column definitions
   )
   ```

 - `350$schemaName.tableName.sql` for **tables without FK**. File content should start with CREATE or ALTER keyword. Note the key field and primary key naming conventions.
 The script will be executed as is, only when the table does not exist.

   ```sql
   CREATE TABLE [schemaName].[tableName](
     tableNameId INT,
     ... -- other column definitions
     CONSTRAINT [pkTableName] PRIMARY KEY CLUSTERED (tableNameId ASC)
   )
   ```

 - `360$schemaName.tableName.sql` for **tables with 1 FK**. File content should start with CREATE or ALTER keyword. Note the foreign field and foreign key naming conventions
and the use of underscore to separate the foreign table name. The script will be executed as is, only when the table does not exist.

   ```sql
   CREATE TABLE [schemaName].[tableName](
     tableNameId INT,
     foreignTableNameId INT,
     ... -- other column definitions
     CONSTRAINT [pkTableName] PRIMARY KEY CLUSTERED (tableNameId ASC),
     CONSTRAINT [fkTableName_foreignTableName] FOREIGN KEY([foreignTableNameId]) REFERENCES [schemaName].[foreignTableName] ([foreignTableNameId])
   )
   ```

 - `370$schemaName.tableName.sql` for **tables with more than 1 FK**. File content should start with CREATE or ALTER keyword. Note the foreign field and foreign key naming conventions
and the use of underscore to separate the foreign table name. The script will be executed as is, only when the table does not exist.

   ```sql
   CREATE TABLE [schemaName].[tableName](
     xxxxForeignTableNameId INT,
     yyyyForeignTableNameId INT,
     ... -- other column definitions
     CONSTRAINT [pkTableName] PRIMARY KEY CLUSTERED (xxxxForeignTableNameId ASC,yyyyForeignTableNameId ASC),
     CONSTRAINT [fkTableName_xxxxForeignTableName] FOREIGN KEY([xxxForeignTableNameId]) REFERENCES [schemaName].[foreignTableName] ([foreignTableNameId]),
     CONSTRAINT [fkTableName_yyyyForeignTableName] FOREIGN KEY([yyyForeignTableNameId]) REFERENCES [schemaName].[foreignTableName] ([foreignTableNameId])
   )
   ```
   
 - `380$schemaName.alterTable.sql` for **altering tables** to add missing columns or modify existing columns that do not involve table recreation.
 This script will be executed on each reconnection to the database.
 
   ```sql
     IF NOT EXISTS(SELECT * FROM sys.columns WHERE Name = N'fieldName' and Object_ID = Object_ID(N'tableName'))
     begin
       ALTER TABLE tableNAme ADD fieldName INT
     end   
   ```   

 - `450$schemaName.functionName.sql` for **functions**. File content should start with CREATE or ALTER keyword.

   ```sql
   CREATE FUNCTION [schemaName].[functionName](
     @parameterName1 INT, 
     @parameterName2 INT
   ) 
   RETURNS type 
   AS
   BEGIN
     ...
   END
   ```

 - `550$schemaName.tableNameInsert.sql` for INSERT **triggers**. File content should start with CREATE or ALTER keyword. Similar pattern can be used other triggers. 

   ```sql
   CREATE TRIGGER [schemaName].[tableNameInsert] 
   ON [schemaName].[tableName] 
   FOR INSERT
   AS
     ...
   ```

 - `650$schemaName.viewName.sql` for **views**. File content should start with CREATE or ALTER keyword. 

   ```sql
   CREATE VIEW [schemaName].[viewName] 
   AS
     ...
   ```

 - `750$schemaName.procedureName.sql` for **stored procedures**. File content should start with CREATE or ALTER keyword. 
Note the mandatory camelCase naming convention for procedure, parameter and result column names. 

   ```sql
   CREATE PROCEDURE [schemaName].[procedureName]
       @paramName1 INT,
       @paramName2 INT,
       @paramName3 INT OUT,
   AS
     ...
     SELECT x columnName1, y columnName2, ...
   ```

 - `850$schemaName.data.sql` for **loading system default data**. Used for any scripts that will insert missing data. The following patterns are recommended:

   ```sql
   IF NOT EXISTS (SELECT * FROM tableName WHERE tableNameId = '...')
   BEGIN
     SET IDENTITY_INSERT tableName ON --in case of presence of identity columns
     INSERT INTO tableName(...) VALUES  (...)
     SET IDENTITY_INSERT tableName OFF --in case of presence of identity columns
   END
   ```

### **Automatic creation of user defined table types** ###

The SQL port has a configuration property named ‘createTT’. This property can be set to boolean or an array of strings, specifying whether user defined table types matching the tables' structure should be automatically created. There are several scenarios: 

 - If the property is set to **true** then user defined table types will
   be automatically created for all tables. e.g
   
```js
   module.exports = {
        type: 'sql',
        createTT: true
        ...
    }
```

 - If the property is set to an array of strings it means that user defined table types will be created for those tables only.

```js
 
    module.exports = {
        type: 'sql',
        createTT: ['schema1.tableName1', 'schema2.tableName2']
        ...
    }
```
 - if the property is omitted or set to **false** then no user defined table types will be created.

The user defined table types that get automatically created will have their names equal to the their corresponding tables' names followed by '**TT**'

E.g. if table's name is **schema.name** then it's corresponding user defined type's name would be **schema.nameTT**

In case a user defined table type with the same name is manually defined in the schema folder then it will have a priority over the automatic one upon schema initialization/upgrade.