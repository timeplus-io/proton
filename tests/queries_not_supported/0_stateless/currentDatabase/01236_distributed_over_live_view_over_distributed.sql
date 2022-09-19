-- Tags: distributed, no-replicated-database, no-parallel, no-fasttest

SET allow_experimental_live_view = 1;

DROP STREAM IF EXISTS lv;
DROP STREAM IF EXISTS visits;
DROP STREAM IF EXISTS visits_layer;

create stream visits (StartDate date) ENGINE MergeTree ORDER BY(StartDate);
create stream visits_layer (StartDate date) ENGINE Distributed(test_cluster_two_shards_localhost,  currentDatabase(), 'visits', rand());

CREATE LIVE VIEW lv AS SELECT * FROM visits_layer ORDER BY StartDate;

create stream visits_layer_lv (StartDate date) ENGINE Distributed(test_cluster_two_shards_localhost,  currentDatabase(), 'lv', rand());

INSERT INTO visits_layer (StartDate) VALUES ('2020-01-01');
INSERT INTO visits_layer (StartDate) VALUES ('2020-01-02');

SELECT * FROM visits_layer_lv;

DROP STREAM visits;
DROP STREAM visits_layer;

DROP STREAM lv;
DROP STREAM visits_layer_lv;
