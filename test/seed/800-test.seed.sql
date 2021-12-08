IF NOT EXISTS (SELECT 1 FROM test.test1) INSERT INTO test.test1 VALUES (1), (2), (3);
IF NOT EXISTS (SELECT 1 FROM test.test2) INSERT INTO test.test2 VALUES (1), (2), (3);
