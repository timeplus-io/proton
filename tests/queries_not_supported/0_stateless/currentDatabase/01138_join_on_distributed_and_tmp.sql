-- Tags: distributed

DROP STREAM IF EXISTS foo_local;
DROP STREAM IF EXISTS foo_distributed;

create stream foo_local (bar uint64)
ENGINE = MergeTree()
ORDER BY tuple();

create stream foo_distributed AS foo_local
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), foo_local);

CREATE TEMPORARY TABLE _tmp_baz (qux uint64);

SELECT * FROM foo_distributed JOIN _tmp_baz ON foo_distributed.bar = _tmp_baz.qux;

DROP STREAM foo_local;
DROP STREAM foo_distributed;
