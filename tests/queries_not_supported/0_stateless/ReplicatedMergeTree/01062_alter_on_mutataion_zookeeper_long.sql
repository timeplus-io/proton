-- Tags: long, zookeeper, no-replicated-database
-- Tag no-replicated-database: Old syntax is not allowed

DROP STREAM IF EXISTS test_alter_on_mutation;

create stream test_alter_on_mutation
(
  date date,
  key uint64,
  value string
)
ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_01062/alter_on_mutation', '1')
ORDER BY key PARTITION BY date;

INSERT INTO test_alter_on_mutation select to_date('2020-01-05'), number, to_string(number) from system.numbers limit 100;
INSERT INTO test_alter_on_mutation select to_date('2020-01-06'), number, to_string(number) from system.numbers limit 100;
INSERT INTO test_alter_on_mutation select to_date('2020-01-07'), number, to_string(number) from system.numbers limit 100;

SELECT sum(cast(value as uint64)) from test_alter_on_mutation;

ALTER STREAM test_alter_on_mutation MODIFY COLUMN value uint64;

SELECT sum(value) from test_alter_on_mutation;

INSERT INTO test_alter_on_mutation select to_date('2020-01-05'), number, to_string(number) from system.numbers limit 100, 100;
INSERT INTO test_alter_on_mutation select to_date('2020-01-06'), number, to_string(number) from system.numbers limit 100, 100;
INSERT INTO test_alter_on_mutation select to_date('2020-01-07'), number, to_string(number) from system.numbers limit 100, 100;

OPTIMIZE TABLE test_alter_on_mutation FINAL;

SELECT sum(value) from test_alter_on_mutation;

ALTER STREAM test_alter_on_mutation MODIFY COLUMN value string;

SELECT sum(cast(value as uint64)) from test_alter_on_mutation;

OPTIMIZE TABLE test_alter_on_mutation FINAL;

SELECT sum(cast(value as uint64)) from test_alter_on_mutation;

ALTER STREAM test_alter_on_mutation ADD COLUMN value1 float64;

SELECT sum(value1) from test_alter_on_mutation;

ALTER STREAM test_alter_on_mutation DROP COLUMN value;

SELECT sum(value) from test_alter_on_mutation; -- {serverError 47}

ALTER STREAM test_alter_on_mutation ADD COLUMN value string DEFAULT '10';

SELECT sum(cast(value as uint64)) from test_alter_on_mutation;

-- TODO(alesap)
OPTIMIZE table test_alter_on_mutation FINAL;

ALTER STREAM test_alter_on_mutation MODIFY COLUMN value uint64 DEFAULT 10;

SELECT sum(value) from test_alter_on_mutation;

DROP STREAM IF EXISTS test_alter_on_mutation;

DROP STREAM IF EXISTS nested_alter;

create stream nested_alter (`d` date, `k` uint64, `i32` int32, `dt` datetime, `n.ui8` array(uint8), `n.s` array(string), `n.d` array(date), `s` string DEFAULT '0') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01062/nested_alter', 'r2', d, k, 8192);

INSERT INTO nested_alter VALUES ('2015-01-01', 6,38,'2014-07-15 13:26:50',[10,20,30],['asd','qwe','qwe'],['2000-01-01','2000-01-01','2000-01-03'],'100500');

SELECT * FROM nested_alter;

ALTER STREAM nested_alter DROP COLUMN `n.d`;

SELECT * FROM nested_alter;

DROP STREAM nested_alter;
