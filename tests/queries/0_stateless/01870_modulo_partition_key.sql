-- Tags: no-parallel

SELECT 'simple partition key:';
DROP STREAM IF EXISTS table1 SYNC;
create stream table1 (id int64, v uint64)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/test/tables/table12', '1', v)
PARTITION BY id % 200 ORDER BY id;
INSERT INTO table1 SELECT number-205, number FROM numbers(10);
INSERT INTO table1 SELECT number-205, number FROM numbers(400, 10);
SELECT to_int64(partition) as p FROM system.parts WHERE table='table1' and database=currentDatabase() ORDER BY p;

select 'where id % 200 = +-2:';
select id from table1 where id % 200 = 2 OR id % 200 = -2 order by id;
select 'where id % 200 > 0:';
select id from table1 where id % 200 > 0 order by id;
select 'where id % 200 < 0:';
select id from table1 where id % 200 < 0 order by id;

SELECT 'tuple as partition key:';
DROP STREAM IF EXISTS table2;
create stream table2 (id int64, v uint64)
ENGINE = MergeTree()
PARTITION BY (to_int32(id / 2) % 3, id % 200) ORDER BY id;
INSERT INTO table2 SELECT number-205, number FROM numbers(10);
INSERT INTO table2 SELECT number-205, number FROM numbers(400, 10);
SELECT partition as p FROM system.parts WHERE table='table2' and database=currentDatabase() ORDER BY p;

SELECT 'recursive modulo partition key:';
DROP STREAM IF EXISTS table3;
create stream table3 (id int64, v uint64)
ENGINE = MergeTree()
PARTITION BY (id % 200, (id % 200) % 10, to_int32(round((id % 200) / 2, 0))) ORDER BY id;
INSERT INTO table3 SELECT number-205, number FROM numbers(10);
INSERT INTO table3 SELECT number-205, number FROM numbers(400, 10);
SELECT partition as p FROM system.parts WHERE table='table3' and database=currentDatabase() ORDER BY p;

DETACH TABLE table3;
ATTACH TABLE table3;
SELECT 'After detach:';
SELECT partition as p FROM system.parts WHERE table='table3' and database=currentDatabase() ORDER BY p;

SELECT 'Indexes:';
DROP STREAM IF EXISTS table4;
create stream table4 (id int64, v uint64, s string,
INDEX a (id * 2, s) TYPE minmax GRANULARITY 3
) ENGINE = MergeTree() PARTITION BY id % 10 ORDER BY v;
INSERT INTO table4 SELECT number, number, to_string(number) FROM numbers(1000);
SELECT count() FROM table4 WHERE id % 10 = 7;

SELECT 'comparison:';
SELECT v, v-205 as vv, modulo(vv, 200), moduloLegacy(vv, 200) FROM table1 ORDER BY v;

DROP STREAM table1 SYNC;
DROP STREAM table2 SYNC;
DROP STREAM table3 SYNC;
DROP STREAM table4 SYNC;
