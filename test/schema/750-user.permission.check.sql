ALTER PROCEDURE [user].[permission.check] --this sp checks if the action is allowed for the current user
    @actionId VARCHAR(100) = NULL, --pass an actionId if only rights for this action are required
    @objectId VARCHAR(100) = NULL, -- pass an objectid if only rights for this object are required
    @callParams XML = NULL, -- parameters
    @meta core.metaDataTT READONLY -- information for the current request
AS
BEGIN TRY
    IF EXISTS (SELECT 1 FROM @meta WHERE frontEnd = 'fake')
        THROW 50000, 'test.securityViolation', 1;
END TRY
BEGIN CATCH
    EXEC [core].[error]
    RETURN 55555
END CATCH
