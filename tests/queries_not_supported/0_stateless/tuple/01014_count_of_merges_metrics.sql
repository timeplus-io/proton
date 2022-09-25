DROP STREAM IF EXISTS new_table_test;
DROP STREAM IF EXISTS check_table_test;

create stream new_table_test(name string) ENGINE = MergeTree ORDER BY name;
INSERT INTO new_table_test VALUES ('test');
create stream check_table_test(value1 uint64, value2 uint64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO check_table_test (value1) SELECT value FROM system.events WHERE event = 'Merge';
OPTIMIZE STREAM new_table_test FINAL;
INSERT INTO check_table_test (value2) SELECT value FROM system.events WHERE event = 'Merge';
SELECT count() FROM check_table_test WHERE value2 > value1;


DROP STREAM new_table_test;
DROP STREAM check_table_test;
