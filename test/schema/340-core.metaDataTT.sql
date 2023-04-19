CREATE TYPE [core].[metaDataTT] AS TABLE(
    [auth.actorId] [BIGINT] NULL,
    [method] [VARCHAR](100) NOT NULL,
    [ipAddress] [NVARCHAR](50) NULL,
    [frontEnd] [NVARCHAR](500) NULL,
    [languageId] BIGINT,
    [debug] BIT,
    [protection] INT DEFAULT (0),
    [globalId] [VARCHAR](36),
    [auth.sessionId] [VARCHAR](36),
    [machineName] [NVARCHAR](50) NULL,
    [hostName] [NVARCHAR](50) NULL,
    [localPort] [BIGINT] NULL,
    [latitude] [decimal](12, 9) NULL,
    [longitude] [decimal](12, 9) NULL,
    [os] [NVARCHAR](50) NULL,
    [version] [NVARCHAR](50) NULL,
    [serviceName] [NVARCHAR](50) NULL,
    [localAddress] [NVARCHAR](50) NULL,
    [deviceId] [NVARCHAR](50) NULL,
    [channel] [NVARCHAR](10) NULL,
    [userName] [VARBINARY](416) NULL,
    [auth.checkSession] BIT NULL,
    [traceId] BINARY(16)
)