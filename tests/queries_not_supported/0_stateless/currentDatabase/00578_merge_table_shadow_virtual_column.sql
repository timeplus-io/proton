DROP STREAM IF EXISTS numbers1;
DROP STREAM IF EXISTS numbers2;

create stream numbers1  AS SELECT number as _table FROM numbers(1000);
create stream numbers2  AS SELECT number as _table FROM numbers(1000);

SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$') WHERE _table='numbers1'; -- { serverError 53 }
SELECT count() FROM merge(currentDatabase(), '^numbers\\d+$') WHERE _table=1;

DROP STREAM numbers1;
DROP STREAM numbers2;
