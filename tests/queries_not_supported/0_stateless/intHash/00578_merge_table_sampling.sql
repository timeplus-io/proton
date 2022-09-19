DROP STREAM IF EXISTS numbers1;
DROP STREAM IF EXISTS numbers2;

create stream numbers1  AS SELECT number FROM numbers(1000);
create stream numbers2  AS SELECT number FROM numbers(1000);

SELECT * FROM merge(currentDatabase(), '^numbers\\d+$') SAMPLE 0.1; -- { serverError 141 }

DROP STREAM numbers1;
DROP STREAM numbers2;

create stream numbers1 ENGINE = MergeTree ORDER BY intHash32(number) SAMPLE BY intHash32(number) AS SELECT number FROM numbers(1000);
create stream numbers2 ENGINE = MergeTree ORDER BY intHash32(number) SAMPLE BY intHash32(number) AS SELECT number FROM numbers(1000);

SELECT * FROM merge(currentDatabase(), '^numbers\\d+$') SAMPLE 0.01;

DROP STREAM numbers1;
DROP STREAM numbers2;
