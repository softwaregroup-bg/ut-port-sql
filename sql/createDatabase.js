module.exports = (name, user) => {
    return `
    IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = '${name}')
    BEGIN
      CREATE DATABASE [${name}]
      ALTER DATABASE [${name}] SET READ_COMMITTED_SNAPSHOT ON
      ALTER DATABASE [${name}] SET AUTO_SHRINK OFF
    END`;
};
