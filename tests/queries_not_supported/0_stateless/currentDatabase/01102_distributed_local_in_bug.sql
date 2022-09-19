-- Tags: distributed

DROP STREAM IF EXISTS hits;
DROP STREAM IF EXISTS visits;
DROP STREAM IF EXISTS hits_layer;
DROP STREAM IF EXISTS visits_layer;

create stream visits(StartDate date) ENGINE MergeTree ORDER BY(StartDate);
create stream hits(EventDate date, WatchID uint8) ENGINE MergeTree ORDER BY(EventDate);

create stream visits_layer(StartDate date) ENGINE Distributed(test_cluster_two_shards_localhost,  currentDatabase(), 'visits');
create stream hits_layer(EventDate date, WatchID uint8) ENGINE Distributed(test_cluster_two_shards_localhost,  currentDatabase(), 'hits');

SET distributed_product_mode = 'local';

SELECT 0 FROM hits_layer AS hl
PREWHERE WatchID IN
(
    SELECT 0 FROM visits_layer AS vl
)
WHERE 0;

DROP STREAM hits;
DROP STREAM visits;
DROP STREAM hits_layer;
DROP STREAM visits_layer;
