# **SQL Port:** `ut-port-sql`

The purpose of this port is to work with SQL Server database by connecting,
creating database, creating schema objects and executing stored procedures.

For connecting to MSSQL it uses the [mssql module](https://github.com/tediousjs/node-mssql)

## Configuration

SQL port is usually initialized like this:

```js
module.exports = function db({config: {test}}) {
    return class db extends require('ut-port-sql')(...arguments) {
        get defaults() {
            return {
                createTT: true,
                retry: false,
                retrySchemaUpdate: true,
                namespace: [
                    'db/moduleName',
                    test && 'moduleNameTest'
                ].filter(value => value),
                imports: [
                    'sql',
                    'utModuleName.sql',
                    'utModuleName.sqlSeed',
                    'utModuleName.sqlStandard',
                    test && 'utModuleName.sqlTest',
                ].filter(value => value),
            };
        }
    };
};
```

Important part of the configuration is specifying connection parameters.
This involves the following configuration:

```yaml
db:
  compatibilityLevel: 120 # optional, the default is 120
  connection:
    server: <db server>
    database: <db name>
    user: <db user>
    password: <db password>
    connectionTimeout: 60000
    requestTimeout: 60000
    # other parameters to pass - see general and tedious options in mssql module
    options: # this can be skipped if the below defaults are acceptable
      encrypt: true
      trustServerCertificate: true
  create:
    user: <user that can create db and the above db user>
    password: <password for the above user>
```

The parameters specified in `connection` are used during normal operation of
the port. Optionally in the `create`, a user and password can be specified
if automatic creation of the database is allowed.

Use the following additional configuration options to work with the alternative
`msnodesqlv8` driver:

```yaml
db:
  connection:
    driver: msnodesqlv8
    TrustServerCertificate: 'yes'
```

See [mssql](https://www.npmjs.com/package/mssql) module docs for more details about
other options to pass.

## **Schema sync**

The SQL port has a configuration property named `schema`. This property can be
set to an array of objects, or functions returning array of objects, specifying
folders that contain source code of database objects.
Here is example:

```js
module.exports = function sql() {
    return {
        // folders containing files, used to create the schemas
        schema: [{
            path: path.join(__dirname, 'schema1'), linkSP: true, exclude
        }, {
            path: path.join(__dirname, 'schema2'), linkSP: false
        }],

        // do not generate table types for the specified tables
        skipTableType: ['schema1.table1']
    }
}
```

This configuration will make the port scan the specified folders and synchronize
database schema. The optional `exclude` property can specify files to be excluded
from synchronization. It can be a string, a regular expression, of array of these.
In addition, procedures from the first folder will be automatically
linked to the port (because of the `linkSP: true` setting), so that they can be
executed directly, based on their schema name and procedure name.
So for the following procedure:

```sql
CREATE PROCEDURE schema1.storedProcedureName
  @param1 int,
  @param2 nvarchar(20)
  @param3 tableType
AS
  ...
```

The stored procedure can be called using this:

```js
bus.importMethod('schema1.storedProcedureName')({
  param1: 1,
  param2: 'value',
  param3:[{
    column1: 'value row 1'
  },{
    column1: 'value row 2'
  }]
})
```

Linking can be fine-tuned with the linkSP configuration property, that can be
set on the port. It can be boolean, function, object, regular expression or
array of the previous types, which will be checked against schema name or full
object name for each procedure. Here are some examples:

```js
module.exports = { linkSP: null || undefined /* link all SP by default */ };
module.exports = { linkSP: false /* do not link any SP */ };
module.exports = { linkSP: true /* link all SP */ };
module.exports = {
  linkSP: ['schema1', 'schema2'] /* link all SP in the listed schemas */
};
module.exports = {
  linkSP: /user\.identity\..*/ /* link all SP matching the pattern */
};
module.exports = { linkSP: ['user.identity.check'] /* link individual SPs */ };
module.exports = {
  linkSP: ({namespace, full}) =>
    shallLinkSP(full) /* link based on result of function call */
};
module.exports = {
  linkSP: [
    'schema1',
    /user\.identity\..*/,
    ({namespace, full}) => shallLinkSP(full)
  ] /* mix of the above */
};
```

The schema sync algorithm matches database objects to files in the file system
and updates the database objects if differences are detected.
The file names in the file system are supposed to follow the following naming:
`xxx-schemaName.objectName.(sql|js|json|yaml)` , where xxx are numbers, which
control the order of execution. The below convention is recommended.
Note the **camelCase** naming convention. File names, including extension are
**case sensitive**.

- `150-schemaName.sql` for **schema** creation:

  ```sql
  CREATE SCHEMA [schemaName]
  ```

- `250-schemaName.synonymName.sql` for **single synonym per file** creation.
  Note that this needs to be in single line in the actual file.
  Following the below exact query format (i.e. using the [ and ] brackets and no
  extra spaces) and filename matches the `xxx-schema.object` pattern. Any
  deviation from the below query format will lead to pointless recreation of synonyms:

  ```sql
  DROP SYNONYM [schemaName].[synonymName] CREATE SYNONYM [schemaName].
  [synonymName] FOR [foreignDatabaseName].[foreignSchemaName].[foreignObjectName]
  ```

- `250-schemaName.scriptName.sql` for **multiple synonyms per file** creation.
  Filename should not match any object name in the database. The recommended
  queries in this case are:

  ```sql
  IF NOT EXISTS (
    SELECT
      *
    FROM
      sys.synonyms sy
    JOIN
      sys.schemas s ON s.schema_id=sy.schema_id AND s.name='schemaName'
    WHERE
      sy.base_objectName=
        '[foreignDatabaseName].[foreignSchemaName].[foreignObjectName]' AND
      sy.type='SN' AND sy.name='synonymName'
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
      sy.type='SN' AND sy.name='synonymName'
    ) DROP SYNONYM [schemaName].[synonymName]
    CREATE SYNONYM [schemaName].[synonymName] FOR [foreignDatabaseName].[foreignSchemaName].[foreignObjectName]
   END
   ```

- `340-schemaName.typeName.sql` for **used defined table type**.
  The script will be executed as is, only when the used defined table type does
  not exist.

  ```sql
  CREATE TYPE [schemaName].[typeName] AS TABLE(
    column1 int,
    ... -- other column definitions
  )
  ```

- `350-schemaName.tableName.sql` for **tables without FK**. File content should
  start with CREATE or ALTER keyword. Note the key field and primary key naming conventions.
  The script will be executed as is, only when the table does not exist.

  ```sql
  CREATE TABLE [schemaName].[tableName](
    tableNameId INT,
    ... -- other column definitions
    CONSTRAINT [pkSchemaNameTableName] PRIMARY KEY CLUSTERED (tableNameId ASC)
    CONSTRAINT [ukSchemaNameTableName_field1_field2]
      UNIQUE ([field1] ASC, [field2] ASC)
  )
  ```

- `360-schemaName.tableName.sql` for **tables with 1 FK**. File content should
  start with CREATE or ALTER keyword. Note the foreign field and foreign key
  naming conventions and the use of underscore to separate the foreign table name.
  The script will be executed as is, only when the table does not exist.

  ```sql
  CREATE TABLE [schemaName].[tableName](
    tableNameId INT,
    foreignTableNameId INT,
    ... -- other column definitions
    CONSTRAINT [pkSchemaNameTableName] PRIMARY KEY CLUSTERED (tableNameId ASC),
    CONSTRAINT [fkSchemaNameTableName_foreignTableName] FOREIGN KEY([foreignTableNameId])
      REFERENCES [schemaName].[foreignTableName] ([foreignTableNameId])
  )
  ```

- `370-schemaName.tableName.sql` for **tables with more than 1 FK**. File
  content should start with CREATE or ALTER keyword. Note the foreign field and
  foreign key naming conventions and the use of underscore to separate the
  foreign table name. The script will be executed as is, only when the table
  does not exist.

  ```sql
  CREATE TABLE [schemaName].[tableName](
    xxxxForeignTableNameId INT,
    yyyyForeignTableNameId INT,
    ... -- other column definitions
    CONSTRAINT [pkSchemaNameTableName]
      PRIMARY KEY CLUSTERED (xxxxForeignTableNameId ASC,yyyyForeignTableNameId ASC),
    CONSTRAINT [fkSchemaNameTableName_xxxxForeignTableName]
      FOREIGN KEY([xxxForeignTableNameId])
      REFERENCES [schemaName].[foreignTableName] ([foreignTableNameId]),
    CONSTRAINT [fkSchemaNameTableName_yyyyForeignTableName]
      FOREIGN KEY([yyyForeignTableNameId])
      REFERENCES [schemaName].[foreignTableName] ([foreignTableNameId])
  )
  ```

- `380-schemaName.alterTable.sql` for **altering tables** to add missing columns
  or modify existing columns that do not involve table recreation. This script
  will be executed on each reconnection to the database.

  ```sql
    IF NOT EXISTS(
      SELECT *
      FROM sys.columns
      WHERE Name = N'fieldName' and Object_ID = Object_ID(N'tableName')
    )
    BEGIN
      ALTER TABLE tableNAme ADD fieldName INT
    END
  ```

- `450-schemaName.functionName.sql` for **functions**. File content should start
  with CREATE or ALTER keyword.

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

- `550-schemaName.tableNameInsert.sql` for INSERT **triggers**. File content
  should start with CREATE or ALTER keyword. Similar pattern can be used other triggers.

  ```sql
  CREATE TRIGGER [schemaName].[tableNameInsert]
  ON [schemaName].[tableName]
  FOR INSERT
  AS
    ...
  ```

- `650-schemaName.viewName.sql` for **views**. File content should start with
  CREATE or ALTER keyword.

   ```sql
   CREATE VIEW [schemaName].[viewName]
   AS
     ...
   ```

- `750-schemaName.procedureName.sql` for **stored procedures**. File content
  should start with CREATE or ALTER keyword. Note the mandatory camelCase naming
  convention for procedure, parameter and result column names.

  ```sql
  CREATE PROCEDURE [schemaName].[procedureName]
      @paramName1 INT,
      @paramName2 INT,
      @paramName3 INT OUT,
  AS
    ...
    SELECT x columnName1, y columnName2, ...
  ```

- `850-schemaName.data.sql` for **loading system default data**.
  `WARNING - THIS IS DEPRECATED.`
  `THE RECOMMENDED APPROACH IS TO LOAD DATA THROUGH *.yaml FILES`
  Used for any scripts that will insert missing data:

  ```sql
  IF NOT EXISTS (SELECT * FROM tableName WHERE tableNameId = '...')
  BEGIN
    SET IDENTITY_INSERT tableName ON --in case of presence of identity columns
    INSERT INTO tableName(...) VALUES  (...)
    SET IDENTITY_INSERT tableName OFF --in case of presence of identity columns
  END
  ```

- `850-schemaName.entity.merge.yaml` for **loading system default data**
  This will call stored procedure, which should have already been created
  by previous files. These are usually procedures, which can safely merge
  the provided data, which is often non normalized and include no DB keys.
  SP will receive parameters, based on the values of the  yaml file:

  ```yaml
  parameter1: value1
  parameter2:
    - {column1: row1value1, column2: row1value2}
    - {column1: row2value1, column2: row2value2}
  ```

## **Automatic creation of user defined table types**

The SQL port has a configuration property named `createTT`. This property can be
set to a boolean or an array of strings, specifying whether user defined table
types matching the tables' structure should be automatically created. There are
several scenarios:

- If the property is set to `createTT: true`, then user defined table types will
   be automatically created for all tables.

- If the property is set to an array of strings
  (e.g. `createTT: ['schema1.tableName1', 'schema2.tableName2']`)
  it means that user defined table types will be created for these tables only.

- If the property is omitted or set to `createTT: false` then no user defined table
  types will be created.

The user defined table types that get automatically created will have their
names equal to the their corresponding tables' names followed by '**TT**' and '**TTU**'

E.g. if table's name is **schema.name** then it's corresponding user defined
table types names would be **schema.nameTT** and **schema.nameTTU**

In case a user defined table type with the same name is manually defined in the
schema folder then it will have a priority over the automatic one upon schema initialization/upgrade.

## **Automatic insertion of permission check snippets**

The SQL port schemas have a configuration property named `permissionCheck`.
This property can be set to boolean or an array of strings,
specifying whether stored procedures defined in the schema
should automatically check the caller permissions.
Note that in addition to this configuration each
stored procedure should have a specific comment
(--ut-permission-check) in its body which to be replaced
with the respective snippet. There are several scenarios:

- If the property is set to **true** then the permission check snippet
will be enabled for all the stored procedures which
have the --ut-permission-check comment somewhere within their body.
e.g

  ```js
    const path = require('path');

    module.exports = function sql() {
        return {
          schema: [
            {
              path: path.join(__dirname, 'schema'),
              permissionCheck: true
            }
          ]
        };
    };
  ```

- If the property is set to an array of strings the permission check snippet
will be enabled for those stored procedures only.

  ```js
    const path = require('path');

    module.exports = function sql() {
        return {
          schema: [
            {
              path: path.join(__dirname, 'schema'),
              permissionCheck: [
                'schema1.entity1.action1',
                'schema1.entity1.action2'
              ]
            }
          ]
        };
    };
  ```

- if the property is omitted or set to **false**
then no permission checking will be enabled.

```sql
--ut-permission-check

/*
  The comment above will be
  automatically replaced with the code below
  (
    note that all the code will be output
    on 1 row so that the line numbers correctness
    in the stack traces can be preserved
  ):
*/

DECLARE
  @actionID VARCHAR(100) = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID),
  @return INT = 0;

EXEC @return = [user].[permission.check]
  @actionId = @actionID,
  @objectId = NULL,
  @meta = @meta;

IF (@return != 0)
  BEGIN RETURN 55555;
END
```

## **Table and column comments**

Table and column comments are parsed and inserted as description in MSSQL.
Single line comment '**--**' after every parameter, field, column or constraint
will be added as description for itself.

Multiline comments between **/***and***/** are added as description for the next
param.

If both single line comment and multiline comment exist, only multiline comment
will be saved.

  ```sql
  CREATE PROCEDURE [test].[db.method] -- Procedure description
      @param1 int, -- param1 description
      /* multiline description
        related to param2 */
      @param2 nvarchar(20)
      @param3 test.tableValue2 READONLY -- param3 description
  AS
      SELECT * from @param1
      IF @error=1 RAISERROR('test.error',11,0)
  ```

In case of table or type, the table description is at the same line as create
statement after the opening bracket.

  ```sql
  CREATE TABLE [schema].[table] ( -- Table description
      /* multiline description
        related to column1 */
      [column1] varchar(200) NOT NULL,
      -- ignored, because multiline description for the same column already exists
      [column2] bigint NOT NULL, -- single line description related to column2
      [column3] varchar(50) NOT NULL
      CONSTRAINT [pk1] PRIMARY KEY
        NONCLUSTERED (sessionId), -- description for constraint are also parsed
      /* multiline is supported
        for constraints too */
      CONSTRAINT [fkSession_User] FOREIGN KEY([actorId]) REFERENCES [user].[user](actorId)
  )
  ```

## **Database diagrams**

Database diagrams are stored in system tables which are not created by default.
This tables will be created if `create.diagram=true` in the port configuration.

### Result set processing

#### Single result set handling

Stored procedures can single result set like this:

```sql
CREATE PROCEDURE [module].[entity.action]
AS
SELECT * FROM module.someTable
```

This procedure can be called in one of the following ways, handling the
procedure's returned non error `result` (which is array of arrays of row
objects) in different ways:

- `bus.importMethod('subject.object.predicate')(params)` - will resolve with `result`
- `bus.importMethod('subject.object.predicate#[^]')(params)` - will resolve with
  `null` or throw error `portSQL.noRowsExpected` if procedure returned more than
   one result set or procedure returned more than 0 rows in the first result set
- `bus.importMethod('subject.object.predicate#[0]')(params)` - will resolve with
  object containing the first row from the first result set (`result[0][0]`) or
  throw `portSQL.oneRowExpected`, if what procedure returned is not exactly one
  result set with exactly one row
- `bus.importMethod('subject.object.predicate#[?]')(params)` - will resolve with
  `null` or object containing the first row from the first result set
  (`result[0][0]`) or throw `portSQL.maxOneRowExpected`, if what procedure
  returned is not exactly one result set with maximum one row
- `bus.importMethod('subject.object.predicate#[+]')(params)` - will array holding
  first result set rows (`result[0]`) or throw `portSQL.minOneRowExpected` if
  what procedure returned is not exactly one result set with one or more rows
- `bus.importMethod('subject.object.predicate#[]')(params)` - will resolve with array
  holding first result set rows [{row}, ...] or throw error
  `portSQL.singleResultsetExpected` if more than one result set is returned

#### Multiple result set handling

When executing stored procedures, the following pattern can be used to return
named result sets:

```sql
CREATE PROCEDURE [module].[entity.action]
AS

SELECT 'result1' resultSetName
SELECT * FROM module.someTable

SELECT 'result2' resultSetName
SELECT * FROM module.someTable
```

Calling `bus.importMethod('subject.object.predicate')(params)` will resolve with
`{result1:[], result2:[]}`

### Crypto Algorithm or `cryptoAlgorithm` param in config will point how password is encrypted

### **Throwing errors with more data**

In case simple error throwing through `RAISERROR` is not suitable, the following
pattern can be used:

```sql
SELECT
    'someData' resultSetName
SELECT
    *
FROM
    someData

SELECT
    'ut-error' resultSetName,
    'someModule.someError' [type],
    @@servername serverName,
    @@version [version]
IF @@trancount > 0 ROLLBACK TRANSACTION -- if rollback is needed
EXEC [core].[error] @type = 'someModule.someError' -- if processing is needed
RETURN 55555
```

This will throw an error of type `someModule.someError`, which will also have
properties set to the selected columns in the `ut-error` result set plus
additional properties named after all the other result sets.
If the specified error type is known, an instance of that error will be thrown.
Otherwise `PortSQL` error will be thrown, with its cause property set to the
returned error.

## Storing and retrieving JSON

Storing of JSON happens automatically as objects are automatically serialized to
JSON when needed. So if a parameter is not based on `xml` or some form of `date`
or `time`, the parameter value is passed to JSON.stringify in cases it is an `Object`
which is not a `Buffer`.

Converting such JSON to object can also happen automatically, if the column name
in the result set has suffix `.json`. In this case the column value is parsed with
JSON.parse and stored in a property without the `.json` suffix. This enables easier
consumption of stored JSON by just renaming the columns in the result set.

## Bulk import and export

SQL port can scan folders for `*.fmt` files and create methods, which invoke the
bulk copy utility [bcp](https://docs.microsoft.com/en-us/sql/tools/bcp-utility)
to import or export big amounts of data in efficient way.
Pass the the list of folders in the `format` configuration property:

```js
{
  format: [{path: path.join(__dirname, 'format')}]
}
```

The folder is scanned for files named:

- `schema.table.fmt` - this will create two methods in the SQL port
  with names `schema.table.import` and `schema.table.export`
- `schema.table.operation.fmt` - this will create one method in the SQL
  port with the name `schema.table.operation`

The files will be used as format files for import or export of data
in or from the `schema.table` table.
These methods accept the following properties in their first argument:

- `command` - the `bcp` command to execute (in / out / format)
  For the `*.import` and `*.export` methods, this is implicitly set to `in` or
  `out` respectively
- `file` - file to use during the import or export
- `firstRow` - start row, the default is `1`
- `lastRow`- end row
- `maxErrors` - maximum number of allowed errors
- `hints` - hints for bulk import
- `tempDir` - temporary folder to use for error messages, the default is `os.tmpdir()`
- `separator` - field separator to use ( useful when generating a format file)
- `terminator`- row terminator to use ( useful when generating a format file)
- `unicode` - allow usage of unicode characters, default is `true`
  ( useful when generating a format file)

For more details see the [bcp documentation](https://docs.microsoft.com/en-us/sql/tools/bcp-utility)

Usually the `file` parameter is enough for successful import or export of data.
Note that files with data should be treated as temporary and be deleted
or encrypted after the bulk operation is finished.

The recommended command for generating format files is:

```bash
bcp schema.table format nul -c -fschema.table.fmt -Uuser -Ppass -Sserver
```

## Encryption of SP parameters

SQL server port will automatically encrypt values of stored procedure parameters
in these cases:

- parameter type is varbinary and size is multiple of 16
- parameter name starts with `'encrypted'`
- if parameter type is table, values of these columns will be encrypted in these
  cases:
  - column type is varbinary and size is multiple of 16
  - column name starts with `'encrypted'`

Since usually the stored procedures use auto generated table types, the
encryption of parameters is achieved easily by just defining columns in the
table, for example:

```sql
CREATE TABLE [schemaName].[tableName](
    [text] VARBINARY(2048),
    [encryptedAttachments] VARBINARY(MAX),
    [encryptedDetails] VARBINARY(MAX),
)
```

## Decryption of SP results

SQL server port will automatically decrypt columns in the result set if:

column type is varbinary and size is multiple of 16
column name starts with `'encrypted'`, in this case the result will include a
column without this prefix.

## Indexing of encrypted values

To enable easier indexing of encrypted values, SQL port will look for
annotations in the `*.sql` files that define the tables and procedures. These
annotations allow creation and usage of n-gram hashes to index and search the
encrypted values.

### Annotating tables

Annotation of tables allows automatic creation of some helper objects.
Annotation uses JSON within multiline comment after the table definition:

```sql
CREATE TABLE [schemaName].[tableName](
    -- list of columns, constraints, etc.
)
/*{
    "ngram": {
        "index": true,
        "search": true
    }
}*/
```

- Setting the `ngram` property will create table type named `[schemaName].[ngramTT]`
that can be used in the stored procedures that want to receive n-grams
- Setting the `ngram.index` to true will:
  - create an index table named `[schemaName].[tableNameIndex]` for storing the n-grams
  - create a table type named `[schemaName].[ngramIndexTT]`
  - create a stored procedure named `[schemaName].[tableNameIndexMerge]` for merging
    n-grams in the index table
- Setting the `ngram.search` to true will create a function named
`[schemaName].[tableNameSearch]` for searching the n-grams index

### Annotating stored procedure parameters

Annotation of stored procedure parameters allows n-grams to be generated and
passed to the stored procedure. Annotation uses JSON within multiline comment
after the parameter definition:

```sql
CREATE PROCEDURE [schemaName].[method]
    @paramName [schemaName].[tableNameTT] READONLY
/*{
    "paramName.text": true
}*/
    @ngram [schemaName].[ngramTT] READONLY
AS
```

The JSON properties define how each encrypted column is to be indexed. It can
include parameters to the n-gram generation. Passing just true activates the
most common one - trigram generation. All generated n-grams will be passed in
the `@ngram` parameter. Each row in the `@ngram` variable will include information
for the parameter name and table row number used during n-gram generation.

Stored procedure must use the `@ngram` parameter to save the passed data to the
corresponding index table. For example if a variable named `@inserted` contains
the rows inserted in the main table, the following code could be used to save
the n-grams in the index:

```sql
INSERT INTO
    [schemaName].tableNameIndex(id, ngram)
SELECT
    i.id, n.ngram
FROM
    (SELECT id, RANK() OVER (ORDER BY id) rnk FROM @inserted) i
JOIN
    @ngram n ON n.row = i.rnk AND n.param = 'paramName'
```

### Searching in n-gram index

To search in the n-gram index, a procedure must annotate a parameter in a way
similar to the procedures that modifies data. Then it can use the automatically
created function or write alternative one to search using the passed n-gram
hashes. Here is an example how the automatically created function can be used:

```sql
CREATE PROCEDURE [schemaName].[search]
    @paramName [schemaName].[tableTT] READONLY
/*{
    "paramName.text": true
}*/
    @ngramTableName [schemaName].[ngramTT] READONLY
AS
DECLARE @ngramCount INT = (SELECT COUNT(*) FROM @ngram)
SELECT
    t.id, t.text
FROM
    [schemaName].[tableName] t
WHERE
    (
        @ngramCount = 0 OR
        t.id IN (SELECT id FROM [schemaName].[tableNameSearch](@ngram, 1))
    )
```

The automatically created search function expects 2 parameters:

- `@ngram` - list of n-gram hashes to search for
- `@fuzzy` - a float value, that specifies how many of the n-grams must match,
  where 1 means 100%. The function returns the list of keys that contain the
  passed n-grams
