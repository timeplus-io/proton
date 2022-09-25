set allow_experimental_map_type = 1;

-- string type
SET query_mode = 'table';
drop stream if exists table_map;
create stream table_map (a Map(string, string)) engine = Memory;
insert into table_map values ({'name':'zhangsan', 'gender':'male'}), ({'name':'lisi', 'gender':'female'});
select a['name'] from table_map;
drop stream if exists table_map;


drop stream if exists table_map;
create stream table_map (a Map(string, uint64)) engine = MergeTree() order by a;
insert into table_map select map('key1', number, 'key2', number * 2) from numbers(1111, 3);
select a['key1'], a['key2'] from table_map;
drop stream if exists table_map;

-- MergeTree Engine
drop stream if exists table_map;
create stream table_map (a Map(string, string), b string) engine = MergeTree() order by a;
insert into table_map values ({'name':'zhangsan', 'gender':'male'}, 'name'), ({'name':'lisi', 'gender':'female'}, 'gender');
select a[b] from table_map;
select b from table_map where a = map('name','lisi', 'gender', 'female');
drop stream if exists table_map;

-- Big Integer type

create stream table_map (d DATE, m Map(int8, UInt256)) ENGINE = MergeTree() order by d;
insert into table_map values ('2020-01-01', map(1, 0, 2, 1));
select * from table_map;
drop stream table_map;

-- Integer type

create stream table_map (d DATE, m Map(int8, int8)) ENGINE = MergeTree() order by d;
insert into table_map values ('2020-01-01', map(1, 0, 2, -1));
select * from table_map;
drop stream table_map;

-- Unsigned int type
drop stream if exists table_map;
create stream table_map(a Map(uint8, uint64), b uint8) Engine = MergeTree() order by b;
insert into table_map select map(number, number+5), number from numbers(1111,4);
select a[b] from table_map;
drop stream if exists table_map;


-- array Type
drop stream if exists table_map;
create stream table_map(a Map(string, array(uint8))) Engine = MergeTree() order by a;
insert into table_map values(map('k1', [1,2,3], 'k2', [4,5,6])), (map('k0', [], 'k1', [100,20,90]));
insert into table_map select map('k1', [number, number + 2, number * 2]) from numbers(6);
insert into table_map select map('k2', [number, number + 2, number * 2]) from numbers(6);
select a['k1'] as col1 from table_map order by col1;
drop stream if exists table_map;

SELECT CAST(([1, 2, 3], ['1', '2', 'foo']), 'Map(uint8, string)') AS map, map[1];

create stream table_map (n uint32, m Map(string, int))
ENGINE = MergeTree ORDER BY n SETTINGS min_bytes_for_wide_part = 0;

-- coversion from tuple(array(K), array(V))
INSERT INTO table_map SELECT number, (array_map(x -> to_string(x), range(number % 10 + 2)), range(number % 10 + 2)) FROM numbers(100000);
-- coversion from array(tuple(K, V))
INSERT INTO table_map SELECT number, array_map(x -> (to_string(x), x), range(number % 10 + 2)) FROM numbers(100000);
SELECT sum(m['1']), sum(m['7']), sum(m['100']) FROM table_map;

DROP STREAM IF EXISTS table_map;

create stream table_map (n uint32, m Map(string, int))
ENGINE = MergeTree ORDER BY n;

-- coversion from tuple(array(K), array(V))
INSERT INTO table_map SELECT number, (array_map(x -> to_string(x), range(number % 10 + 2)), range(number % 10 + 2)) FROM numbers(100000);
-- coversion from array(tuple(K, V))
INSERT INTO table_map SELECT number, array_map(x -> (to_string(x), x), range(number % 10 + 2)) FROM numbers(100000);
SELECT sum(m['1']), sum(m['7']), sum(m['100']) FROM table_map;

DROP STREAM IF EXISTS table_map;

SELECT CAST(([2, 1, 1023], ['', '']), 'Map(uint8, string)') AS map, map[10] -- { serverError 53}
