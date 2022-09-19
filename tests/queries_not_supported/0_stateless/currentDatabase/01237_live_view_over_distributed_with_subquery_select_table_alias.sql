-- Tags: distributed, no-replicated-database, no-parallel, no-fasttest

SET allow_experimental_live_view = 1;
SET insert_distributed_sync = 1;

DROP STREAM IF EXISTS lv;
DROP STREAM IF EXISTS visits;
DROP STREAM IF EXISTS visits_layer;

create stream visits(StartDate date) ENGINE MergeTree ORDER BY(StartDate);
create stream visits_layer(StartDate date) ENGINE Distributed(test_cluster_two_shards_localhost,  currentDatabase(), 'visits', rand());

CREATE LIVE VIEW lv AS SELECT foo.x FROM (SELECT StartDate AS x FROM visits_layer) AS foo ORDER BY foo.x;

INSERT INTO visits_layer (StartDate) VALUES ('2020-01-01');
INSERT INTO visits_layer (StartDate) VALUES ('2020-01-02');

SELECT * FROM lv;

DROP STREAM visits;
DROP STREAM visits_layer;

DROP STREAM lv;
