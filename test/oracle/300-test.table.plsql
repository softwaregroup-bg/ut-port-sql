CREATE TABLE "test.table"(
    "transferId" NUMBER(19) GENERATED BY DEFAULT AS IDENTITY(START WITH 1000) NOT NULL,
    "transferTypeId" NUMBER(19) NOT NULL,
    "acquirerCode" VARCHAR2(50),
    "transferIdAcquirer" VARCHAR2(50),
    "transferIdLedger" VARCHAR2(50),
    "transferIdIssuer" VARCHAR2(50),
    "transferIdMerchant" VARCHAR2(50),
    "transferDateTime" TIMESTAMP(3) NOT NULL,
    "localDateTime" VARCHAR2(14),
    "settlementDate" DATE,
    "channelId" NUMBER(19) NOT NULL,
    "channelType" VARCHAR2(50) NOT NULL,
    "ordererId" NUMBER(19),
    "merchantId" VARCHAR2(50),
    "merchantInvoice" VARCHAR2(50),
    "merchantPort" VARCHAR2(50),
    "merchantType" VARCHAR2(50),
    "cardId" NUMBER(19),
    "credentialId" VARCHAR2(50),
    "sourceAccount" VARCHAR2(50),
    "destinationAccount" VARCHAR2(50),
    "expireTime" TIMESTAMP(3),
    "expireCount" NUMBER(10),
    "expireCountLedger" NUMBER(10),
    "reversed" NUMBER(1) NOT NULL,
    "reversedLedger" NUMBER(1) NOT NULL,
    "retryTime" TIMESTAMP(3),
    "retryCount" NUMBER(10),
    "ledgerTxState" NUMBER(3),
    "issuerTxState" NUMBER(3),
    "acquirerTxState" NUMBER(3),
    "merchantTxState" NUMBER(3),
    "issuerId" VARCHAR2(50),
    "ledgerId" VARCHAR2(50),
    "transferCurrency" VARCHAR2(3) NOT NULL,
    "transferAmount" NUMBER(19,4) NOT NULL,
    "acquirerFee" NUMBER(19,4),
    "issuerFee" NUMBER(19,4),
    "transferFee" NUMBER(19,4),
    "retrievalReferenceNumber" VARCHAR2(12),
    "description" NVARCHAR2(250),
    "issuerSerialNumber" NUMBER(19),
    "replacementAmount" NUMBER(19,4),
    "replacementAmountCurrency" VARCHAR2(3),
    "actualAmount" NUMBER(19,4),
    "actualAmountCurrency" VARCHAR2(3),
    "settlementAmount" NUMBER(19,4),
    "settlementAmountCurrency" VARCHAR2(3),
    "processorFee" NUMBER(19,4),
    "issuerRequestedDateTime" TIMESTAMP(3),
    "beneficiaryId" NUMBER(19),
    "externalBeneficiaryId" VARCHAR2(50),
    "verificationMethodId" NUMBER(19),
    CONSTRAINT "pkTestTable" PRIMARY KEY ("transferId")
)
