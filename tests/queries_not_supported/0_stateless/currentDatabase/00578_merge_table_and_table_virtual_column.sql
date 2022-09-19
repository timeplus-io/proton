DROP STREAM IF EXISTS numbers1;
DROP STREAM IF EXISTS numbers2;
DROP STREAM IF EXISTS numbers3;
DROP STREAM IF EXISTS numbers4;
DROP STREAM IF EXISTS numbers5;

create stream numbers1 ENGINE = StripeLog AS SELECT number FROM numbers(1000);
create stream numbers2  AS SELECT number FROM numbers(1000);
create stream numbers3   AS SELECT number FROM numbers(1000);
create stream numbers4  AS SELECT number FROM numbers(1000);
create stream numbers5 ENGINE = MergeTree ORDER BY number AS SELECT number FROM numbers(1000);

SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$');
SELECT DISTINCT count() FROM merge(currentDatabase(), '^numbers\\d+$') GROUP BY number;

SET max_rows_to_read = 1000;

SET max_threads = 'auto';
SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$') WHERE _table = 'numbers1';

SET max_threads = 1;
SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$') WHERE _table = 'numbers2';

SET max_threads = 10;
SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$') WHERE _table = 'numbers3';

SET max_rows_to_read = 1;

SET max_threads = 'auto';
SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$') WHERE _table = 'non_existing';

SET max_threads = 1;
SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$') WHERE _table = 'non_existing';

SET max_threads = 10;
SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$') WHERE _table = 'non_existing';

DROP STREAM numbers1;
DROP STREAM numbers2;
DROP STREAM numbers3;
DROP STREAM numbers4;
DROP STREAM numbers5;
