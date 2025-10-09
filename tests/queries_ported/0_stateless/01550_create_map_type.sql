-- string type
drop stream if exists table_map;
create stream table_map (a map(string, string)) engine = Memory;
insert into table_map values ({'name':'zhangsan', 'gender':'male'}), ({'name':'lisi', 'gender':'female'});
select a['name'] from table_map;
drop stream if exists table_map;


drop stream if exists table_map;
create stream table_map (a map(string, uint64)) engine = MergeTree() order by a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into table_map select map_cast('key1', number, 'key2', number * 2) from numbers(1111, 3);
select a['key1'], a['key2'] from table_map;
drop stream if exists table_map;

-- MergeTree Engine
drop stream if exists table_map;
create stream table_map (a map(string, string), b string) engine = MergeTree() order by a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into table_map values ({'name':'zhangsan', 'gender':'male'}, 'name'), ({'name':'lisi', 'gender':'female'}, 'gender');
select a[b] from table_map;
select b from table_map where a = map_cast('name','lisi', 'gender', 'female');
drop stream if exists table_map;

-- Big integer type

create stream table_map (d DATE, m map(int8, uint256)) ENGINE = MergeTree() order by d SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into table_map values ('2020-01-01', map_cast(1, 0, 2, 1));
select * from table_map;
drop stream table_map;

-- integer type

create stream table_map (d DATE, m map(int8, int8)) ENGINE = MergeTree() order by d SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into table_map values ('2020-01-01', map_cast(1, 0, 2, -1));
select * from table_map;
drop stream table_map;

-- Unsigned int type
drop stream if exists table_map;
create stream table_map(a map(uint8, uint64), b uint8) Engine = MergeTree() order by b SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into table_map select map_cast(number, number+5), number from numbers(1111,4);
select a[b] from table_map;
drop stream if exists table_map;


-- array Type
drop stream if exists table_map;
create stream table_map(a map(string, array(uint8))) Engine = MergeTree() order by a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into table_map values(map_cast('k1', [1,2,3], 'k2', [4,5,6])), (map_cast('k0', [], 'k1', [100,20,90]));
-- insert into table_map select map_cast('k1', [number, number + 2, number * 2]) from numbers(6);
-- insert into table_map select map_cast('k2', [number, number + 2, number * 2]) from numbers(6);
select a['k1'] as col1 from table_map order by col1;
drop stream if exists table_map;

SELECT CAST(([1, 2, 3], ['1', '2', 'foo']), 'map(uint8, string)') AS map, map[1];

CREATE STREAM table_map (n uint32, m map(string, int))
ENGINE = MergeTree ORDER BY n SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi';

-- coversion from tuple(array(K), array(V))
INSERT INTO table_map SELECT number, (array_map(x -> to_string(x), range(number % 10 + 2)), range(number % 10 + 2)) FROM numbers(100000);
-- coversion from array(tuple(K, V))
INSERT INTO table_map SELECT number, array_map(x -> (to_string(x), x), range(number % 10 + 2)) FROM numbers(100000);
SELECT sum(m['1']), sum(m['7']), sum(m['100']) FROM table_map;

DROP STREAM IF EXISTS table_map;

CREATE STREAM table_map (n uint32, m map(string, int))
ENGINE = MergeTree ORDER BY n SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

-- coversion from tuple(array(K), array(V))
INSERT INTO table_map SELECT number, (array_map(x -> to_string(x), range(number % 10 + 2)), range(number % 10 + 2)) FROM numbers(100000);
-- coversion from array(tuple(K, V))
INSERT INTO table_map SELECT number, array_map(x -> (to_string(x), x), range(number % 10 + 2)) FROM numbers(100000);
SELECT sum(m['1']), sum(m['7']), sum(m['100']) FROM table_map;

DROP STREAM IF EXISTS table_map;

SELECT CAST(([2, 1, 1023], ['', '']), 'map(uint8, string)') AS map, map[10] -- { serverError TYPE_MISMATCH}
