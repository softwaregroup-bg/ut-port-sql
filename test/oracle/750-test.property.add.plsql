CREATE OR REPLACE PROCEDURE "test.property.add" (
    property IN "test"."propertyTT",
    resultCursor OUT SYS_REFCURSOR
)
AS
    resultName SYS_REFCURSOR;
    resultRows SYS_REFCURSOR;
BEGIN
  -- insert data from nested table type
  FORAll rec in 1..property.count INSERT INTO "test.property" VALUES property(rec);

  -- return resultset name
  OPEN resultName FOR SELECT 'result' "resultSetName" FROM DUAL;
  DBMS_SQL.RETURN_RESULT(resultName);

  -- return inserted data
  OPEN resultRows FOR SELECT "id", "name", "value" FROM "test.property" WHERE "id" IN (SELECT "id" FROM TABLE(property));
  DBMS_SQL.RETURN_RESULT(resultRows);

  -- return inserted data as cursor
  OPEN resultCursor FOR SELECT "id", "name", "value" FROM "test.property" WHERE "id" IN (SELECT "id" FROM TABLE(property));
END;
