-- Tags: distributed, no-replicated-database, no-parallel, no-fasttest

set insert_distributed_sync = 1;
SET allow_experimental_live_view = 1;

DROP STREAM IF EXISTS lv;
DROP STREAM IF EXISTS visits;
DROP STREAM IF EXISTS visits_layer;

create stream visits(StartDate date) ENGINE MergeTree ORDER BY(StartDate);
create stream visits_layer(StartDate date) ENGINE Distributed(test_cluster_two_shards_localhost,  currentDatabase(), 'visits', rand());

CREATE LIVE VIEW lv AS SELECT * FROM visits_layer ORDER BY StartDate;

INSERT INTO visits_layer (StartDate) VALUES ('2020-01-01');
INSERT INTO visits_layer (StartDate) VALUES ('2020-01-02');

SELECT * FROM lv;

DROP STREAM visits;
DROP STREAM visits_layer;

DROP STREAM lv;
