-- Tags: distributed

DROP STREAM IF EXISTS local_00952;
DROP STREAM IF EXISTS distributed_00952;

--
-- insert_allow_materialized_columns=0
--
SELECT 'insert_allow_materialized_columns=0';
SET insert_allow_materialized_columns=0;

--
-- insert_distributed_sync=0
--
SELECT 'insert_distributed_sync=0';
SET insert_distributed_sync=0;

create stream local_00952 (date date, value date MATERIALIZED to_date('2017-08-01')) ENGINE = MergeTree(date, date, 8192);
create stream distributed_00952 AS local_00952 ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), local_00952, rand());

INSERT INTO distributed_00952 VALUES ('2018-08-01');
SYSTEM FLUSH DISTRIBUTED distributed_00952;

SELECT * FROM distributed_00952;
SELECT date, value FROM distributed_00952;
SELECT * FROM local_00952;
SELECT date, value FROM local_00952;

DROP STREAM distributed_00952;
DROP STREAM local_00952;

--
-- insert_distributed_sync=1
--
SELECT 'insert_distributed_sync=1';
SET insert_distributed_sync=1;

create stream local_00952 (date date, value date MATERIALIZED to_date('2017-08-01')) ENGINE = MergeTree(date, date, 8192);
create stream distributed_00952 AS local_00952 ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), local_00952, rand());

INSERT INTO distributed_00952 VALUES ('2018-08-01');

SELECT * FROM distributed_00952;
SELECT date, value FROM distributed_00952;
SELECT * FROM local_00952;
SELECT date, value FROM local_00952;

DROP STREAM distributed_00952;
DROP STREAM local_00952;

--
-- insert_allow_materialized_columns=1
--
SELECT 'insert_allow_materialized_columns=1';
SET insert_allow_materialized_columns=1;

--
-- insert_distributed_sync=0
--
SELECT 'insert_distributed_sync=0';
SET insert_distributed_sync=0;

create stream local_00952 (date date, value date MATERIALIZED to_date('2017-08-01')) ENGINE = MergeTree(date, date, 8192);
create stream distributed_00952 AS local_00952 ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), local_00952, rand());

INSERT INTO distributed_00952 (date, value) VALUES ('2018-08-01', '2019-08-01');
SYSTEM FLUSH DISTRIBUTED distributed_00952;

SELECT * FROM distributed_00952;
SELECT date, value FROM distributed_00952;
SELECT * FROM local_00952;
SELECT date, value FROM local_00952;

DROP STREAM distributed_00952;
DROP STREAM local_00952;

--
-- insert_distributed_sync=1
--
SELECT 'insert_distributed_sync=1';
SET insert_distributed_sync=1;

create stream local_00952 (date date, value date MATERIALIZED to_date('2017-08-01')) ENGINE = MergeTree(date, date, 8192);
create stream distributed_00952 AS local_00952 ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), local_00952, rand());

INSERT INTO distributed_00952 (date, value) VALUES ('2018-08-01', '2019-08-01');

SELECT * FROM distributed_00952;
SELECT date, value FROM distributed_00952;
SELECT * FROM local_00952;
SELECT date, value FROM local_00952;

DROP STREAM distributed_00952;
DROP STREAM local_00952;
