SET query_mode = 'table';
drop stream if exists tab_00650;
create stream tab_00650 (val uint32, n nested(x uint8, y string)) engine = Memory;
insert into tab_00650 values (1, [1, 2, 1, 1, 2, 1], ['a', 'a', 'b', 'a', 'b', 'b']);
select array_enumerate_uniq(n.x) from tab_00650;
select array_enumerate_uniq(n.y) from tab_00650;
select array_enumerate_uniq(n.x, n.y) from tab_00650;
select array_enumerate_uniq(array_map((a, b) -> (a, b), n.x, n.y)) from tab_00650;
select array_enumerate_uniq(array_map((a, b) -> (a, b), n.x, n.y), n.x) from tab_00650;
select array_enumerate_uniq(array_map((a, b) -> (a, b), n.x, n.y), array_map((a, b) -> (b, a), n.x, n.y)) from tab_00650;

drop stream tab_00650;
create stream tab_00650 (val uint32, n nested(x Nullable(uint8), y string)) engine = Memory;
insert into tab_00650 values (1, [1, Null, 2, 1, 1, 2, 1, Null, Null], ['a', 'a', 'a', 'b', 'a', 'b', 'b', 'b', 'a']);
select array_enumerate_uniq(n.x) from tab_00650;
select array_enumerate_uniq(n.y) from tab_00650;
select array_enumerate_uniq(n.x, n.y) from tab_00650;
select array_enumerate_uniq(array_map((a, b) -> (a, b), n.x, n.y)) from tab_00650;
select array_enumerate_uniq(array_map((a, b) -> (a, b), n.x, n.y), n.x) from tab_00650;
select array_enumerate_uniq(array_map((a, b) -> (a, b), n.x, n.y), array_map((a, b) -> (b, a), n.x, n.y)) from tab_00650;

drop stream tab_00650;
