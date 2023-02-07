SET allow_deprecated_syntax_for_merge_tree=1;
DROP STREAM IF EXISTS test54378;
CREATE STREAM test54378 (`part_date` Date, `pk_date` Date, `date` Date) ENGINE = MergeTree(part_date, pk_date, 8192);
INSERT INTO test54378 values ('2018-04-19', '2018-04-19', '2018-04-19');
SELECT 232 FROM test54378 PREWHERE (part_date = (SELECT to_date('2018-04-19'))) IN (SELECT to_date('2018-04-19')) GROUP BY to_date(to_date(-2147483649, NULL), NULL), -inf;
DROP STREAM test54378;

