SET check_query_single_value_result = 0;
DROP STREAM IF EXISTS mt_table;

create stream mt_table (d date, key uint64, data string) ENGINE = MergeTree() PARTITION BY to_YYYYMM(d) ORDER BY key;

CHECK TABLE mt_table;

INSERT INTO mt_table VALUES (to_date('2019-01-02'), 1, 'Hello'), (to_date('2019-01-02'), 2, 'World');

CHECK TABLE mt_table;

INSERT INTO mt_table VALUES (to_date('2019-01-02'), 3, 'quick'), (to_date('2019-01-02'), 4, 'brown');

SELECT '========';

CHECK TABLE mt_table;

OPTIMIZE TABLE mt_table FINAL;

SELECT '========';

CHECK TABLE mt_table;

SELECT '========';

INSERT INTO mt_table VALUES (to_date('2019-02-03'), 5, '!'), (to_date('2019-02-03'), 6, '?');

CHECK TABLE mt_table;

SELECT '========';

INSERT INTO mt_table VALUES (to_date('2019-02-03'), 7, 'jump'), (to_date('2019-02-03'), 8, 'around');

OPTIMIZE TABLE mt_table FINAL;

CHECK TABLE mt_table PARTITION 201902;

DROP STREAM IF EXISTS mt_table;
