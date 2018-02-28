module.exports = (name, user, password) => {
    return `
    IF NOT EXISTS (SELECT name FROM master.sys.server_principals WHERE name = '${user}')
    BEGIN
        CREATE LOGIN [${user}] WITH PASSWORD = N'${password}', CHECK_POLICY = OFF
    END
    USE [${name}]
    IF NOT EXISTS (SELECT name FROM sys.database_principals WHERE name = '${user}')
    BEGIN
        CREATE USER [${user}] FOR LOGIN [${user}]
    END
    IF (is_rolemember('db_owner', '${user}') IS NULL OR is_rolemember('db_owner', '${user}') = 0)
    BEGIN
        EXEC sp_addrolemember 'db_owner', '${user}'
    END
    IF NOT EXISTS (SELECT 1 FROM sys.server_principals AS pr
        INNER JOIN sys.server_permissions AS pe ON pe.grantee_principal_id = pr.principal_id
        WHERE permission_name = N'VIEW SERVER STATE' AND state = N'G' AND pr.name = N'${user}')
    BEGIN
        USE [master]
        GRANT VIEW SERVER STATE to [${user}]
    END`;
};
