CREATE OR REPLACE PROCEDURE "test.procedure" (
    testInput IN VARCHAR2,
    testInputOutput IN OUT VARCHAR2,
    testOutputNumber OUT NUMBER,
    testOutputString OUT VARCHAR2
)
AS
BEGIN
  testInputOutput := testInput || '/' || testInputOutput;
  testOutputNumber := 555;
  testOutputString := 'output-string';
END;
