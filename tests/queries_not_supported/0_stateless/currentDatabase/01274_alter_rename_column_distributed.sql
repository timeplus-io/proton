-- Tags: distributed

set insert_distributed_sync = 1;

DROP STREAM IF EXISTS visits;
DROP STREAM IF EXISTS visits_dist;

create stream visits(StartDate date, Name string) ENGINE MergeTree ORDER BY(StartDate);
create stream visits_dist AS visits ENGINE Distributed(test_cluster_two_shards_localhost,  currentDatabase(), 'visits', rand());

INSERT INTO visits_dist (StartDate, Name) VALUES ('2020-01-01', 'hello');
INSERT INTO visits_dist (StartDate, Name) VALUES ('2020-01-02', 'hello2');

ALTER STREAM visits RENAME COLUMN Name TO Name2;
ALTER STREAM visits_dist RENAME COLUMN Name TO Name2;

SELECT * FROM visits_dist ORDER BY StartDate, Name2;

DROP STREAM visits;
DROP STREAM visits_dist;

