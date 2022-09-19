DROP STREAM IF EXISTS sparse_tuple;

create stream sparse_tuple (id uint64, t tuple(a uint64, s string))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;

INSERT INTO sparse_tuple SELECT number, (if (number % 20 = 0, number, 0), repeat('a', number % 10 + 1)) FROM numbers(1000);

SELECT column, subcolumns.names, subcolumns.types, subcolumns.serializations
FROM system.parts_columns
WHERE table = 'sparse_tuple' AND database = currentDatabase()
ORDER BY column;

SELECT t FROM sparse_tuple ORDER BY id LIMIT 5;
SELECT t FROM sparse_tuple WHERE t.a != 0 ORDER BY id LIMIT 5;
SELECT t FROM sparse_tuple WHERE t.a != 0 ORDER BY t.a LIMIT 5;

SELECT t.a FROM sparse_tuple ORDER BY id LIMIT 5;
SELECT t.a FROM sparse_tuple WHERE t.a != 0 ORDER BY id LIMIT 5;
SELECT t.a FROM sparse_tuple WHERE t.a != 0 ORDER BY t.a LIMIT 5;

SELECT t.s FROM sparse_tuple ORDER BY id LIMIT 5;
SELECT t.s FROM sparse_tuple WHERE t.a != 0 ORDER BY id LIMIT 5;

DROP STREAM IF EXISTS sparse_tuple;

create stream sparse_tuple (id uint64, t tuple(a uint64, b tuple(u uint32, s string)))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;

INSERT INTO sparse_tuple SELECT number, (if (number % 20 = 0, number, 0), (if (number % 15 = 0, number, 0), repeat('a', number % 10 + 1))) FROM numbers(1000);

SELECT column, subcolumns.names, subcolumns.types, subcolumns.serializations
FROM system.parts_columns
WHERE table = 'sparse_tuple' AND database = currentDatabase()
ORDER BY column;

SELECT t.a FROM sparse_tuple WHERE t.b.u != 0 ORDER BY id LIMIT 5;

SELECT t.b.s FROM sparse_tuple ORDER BY id LIMIT 5;
SELECT t.b.s FROM sparse_tuple WHERE t.b.u != 0 ORDER BY id LIMIT 5;

DETACH TABLE sparse_tuple;
ATTACH TABLE sparse_tuple;

SELECT column, subcolumns.names, subcolumns.types, subcolumns.serializations
FROM system.parts_columns
WHERE table = 'sparse_tuple' AND database = currentDatabase()
ORDER BY column;

SELECT t.b.s FROM sparse_tuple WHERE t.b.u != 0 ORDER BY id LIMIT 5;

DROP STREAM IF EXISTS sparse_tuple;
