CREATE TABLE "test.property"(--table that stores item properties
    "id" NUMBER(19) NOT NULL, --id of item
    "name" NVARCHAR2(50) NOT NULL, --property name
    "value" NVARCHAR2(200), --property value
    CONSTRAINT "pkTestProperty" PRIMARY KEY ("id", "name")
)
