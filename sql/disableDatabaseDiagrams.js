module.exports = (name) => {
    return `
    USE [${name}]

    IF OBJECT_ID(N'dbo.fn_diagramobjects') IS NULL and IS_MEMBER('db_owner') = 1
        DROP FUNCTION dbo.fn_diagramobjects

    IF OBJECT_ID(N'dbo.sp_dropdiagram') IS NULL and IS_MEMBER('db_owner') = 1
        DROP PROCEDURE dbo.sp_dropdiagram

    IF OBJECT_ID(N'dbo.sp_alterdiagram') IS NULL and IS_MEMBER('db_owner') = 1
        DROP PROCEDURE dbo.sp_alterdiagram

    IF OBJECT_ID(N'dbo.sp_renamediagram') IS NULL and IS_MEMBER('db_owner') = 1
        DROP PROCEDURE dbo.sp_renamediagram

    IF OBJECT_ID(N'dbo.sp_creatediagram') IS NULL and IS_MEMBER('db_owner') = 1
        DROP PROCEDURE dbo.sp_creatediagram

    IF OBJECT_ID(N'dbo.sp_helpdiagramdefinition') IS NULL and IS_MEMBER('db_owner') = 1
        DROP PROCEDURE dbo.sp_helpdiagramdefinition

    IF OBJECT_ID(N'dbo.sp_helpdiagrams') IS NULL and IS_MEMBER('db_owner') = 1
        DROP PROCEDURE dbo.sp_helpdiagrams

    IF OBJECT_ID(N'dbo.sysdiagrams') IS NOT NULL and IS_MEMBER('db_owner') = 1
        DROP TABLE dbo.sysdiagrams

    IF OBJECT_ID(N'dbo.sp_upgraddiagrams') IS NULL and IS_MEMBER('db_owner') = 1
        DROP PROCEDURE dbo.sp_upgraddiagrams`;
};
