DROP STREAM IF EXISTS mmm;

create stream mmm ENGINE=MergeTree ORDER BY number
AS SELECT number, rand() % 10 AS a FROM numbers(1000);

ALTER STREAM mmm DELETE WHERE a IN (SELECT a FROM mmm) SETTINGS mutations_sync=1;

SELECT is_done FROM system.mutations WHERE table = 'mmm' and database=currentDatabase();

SELECT * FROM mmm;

DROP STREAM IF EXISTS mmm;
