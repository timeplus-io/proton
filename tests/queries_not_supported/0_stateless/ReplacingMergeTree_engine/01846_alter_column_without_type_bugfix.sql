DROP STREAM IF EXISTS alter_test;
create stream alter_test (a int32, b datetime) ENGINE = ReplacingMergeTree(b) ORDER BY a;
ALTER STREAM alter_test MODIFY COLUMN `b` datetime DEFAULT now();
ALTER STREAM alter_test MODIFY COLUMN `b` DEFAULT now() + 1;
SHOW create stream alter_test;
DROP STREAM alter_test;
