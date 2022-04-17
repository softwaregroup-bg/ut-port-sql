CREATE OR REPLACE PROCEDURE "test.procedure" (
    testInput IN VARCHAR2,
    testInputOutput IN OUT VARCHAR2,
    testOutput OUT NUMBER
)
AS
BEGIN
  testInputOutput := testInput || testInputOutput;
  testOutput := 101;
END;
