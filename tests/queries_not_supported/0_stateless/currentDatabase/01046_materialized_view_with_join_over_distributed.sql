-- Tags: distributed

-- from https://github.com/ClickHouse/ClickHouse/issues/5142

set insert_distributed_sync = 1;

DROP STREAM IF EXISTS t;
DROP STREAM IF EXISTS t_d;
DROP STREAM IF EXISTS t_v;
create stream t (`A` Int64) ENGINE = MergeTree() ORDER BY tuple();
create stream t_d AS t ENGINE = Distributed(test_shard_localhost, currentDatabase(), t);
CREATE MATERIALIZED VIEW t_v ENGINE = MergeTree() ORDER BY tuple() AS SELECT A FROM t LEFT JOIN ( SELECT toInt64(dummy) AS A FROM system.one ) js2 USING (A);

INSERT INTO t_d SELECT number FROM numbers(2);
SELECT * FROM t_v ORDER BY A;

INSERT INTO t SELECT number+2 FROM numbers(2);
SELECT * FROM t_v ORDER BY A;

DROP STREAM IF EXISTS t_v;
DROP STREAM IF EXISTS t_d;
DROP STREAM IF EXISTS t;
