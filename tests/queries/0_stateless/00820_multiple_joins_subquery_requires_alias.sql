DROP STREAM IF EXISTS table1;
DROP STREAM IF EXISTS table2;
DROP STREAM IF EXISTS table3;
DROP STREAM IF EXISTS table5;

create stream table1 (a uint32) ;
create stream table2 (a uint32, b uint32) ;
create stream table3 (b uint32, c uint32) ;
create stream table5 (a uint32, b uint32, c uint32) ;

INSERT INTO table1 SELECT number FROM numbers(21);
INSERT INTO table2 SELECT number * 2, number * 20 FROM numbers(11);
INSERT INTO table3 SELECT number * 30, number * 300 FROM numbers(10);
INSERT INTO table5 SELECT number * 5, number * 50, number * 500 FROM numbers(10);

SET joined_subquery_requires_alias = 1;

select t1.a, t2.b, t3.c from table1 as t1 join table2 as t2 on t1.a = t2.a join table3 as t3 on t2.b = t3.b;
select t1.a, t2.b, t5.c from table1 as t1 join table2 as t2 on t1.a = t2.a join table5 as t5 on t1.a = t5.a AND t2.b = t5.b;

select t1.a, t2.a, t2.b, t3.b, t3.c, t5.a, t5.b, t5.c
from table1 as t1
join table2 as t2 on t1.a = t2.a
join table3 as t3 on t2.b = t3.b
join table5 as t5 on t3.c = t5.c
FORMAT PrettyCompactNoEscapes;

select t1.a as t1_a, t2.a as t2_a, t2.b as t2_b, t3.b as t3_b
from table1 as t1
join table2 as t2 on t1_a = t2_a
join table3 as t3 on t2_b = t3_b;

select t1.a as t1_a, t2.a as t2_a, t2.b as t2_b, t3.b as t3_b
from table1 as t1
join table2 as t2 on t1.a = t2.a
join table3 as t3 on t2.b = t3.b;

select t1.a as t1_a, t2.a as t2_a, t2.b as t2_b, t3.b as t3_b
from table1 as t1
join table2 as t2 on table1.a = table2.a
join table3 as t3 on table2.b = table3.b;

select t1.a, t2.a, t2.b, t3.b
from table1 as t1
join table2 as t2 on table1.a = table2.a
join table3 as t3 on table2.b = table3.b;

select t1.a, t2.a, t2.b, t3.b
from table1 as t1
join table2 as t2 on t1.a = t2.a
join table3 as t3 on t2.b = t3.b;

select table1.a, table2.a, table2.b, table3.b
from table1 as t1
join table2 as t2 on table1.a = table2.a
join table3 as t3 on table2.b = table3.b;

select t1.*, t2.*, t3.*
from table1 as t1
join table2 as t2 on table1.a = table2.a
join table3 as t3 on table2.b = table3.b
FORMAT PrettyCompactNoEscapes;

select *
from table1 as t1
join table2 as t2 on t1.a = t2.a
join table3 as t3 on t2.b = t3.b
FORMAT PrettyCompactNoEscapes;

select t1.a as t1_a, t2.a as t2_a, t2.b as t2_b, t3.b as t3_b,
    (t1.a + table2.b) as t1_t2_x, (table1.a + table3.b) as t1_t3_x, (t2.b + t3.b) as t2_t3_x
from table1 as t1
join table2 as t2 on t1_a = t2_a
join table3 as t3 on t2_b = t3_b;

--select (t1.a + table2.b) as t1_t2_x, (table1.a + table3.b) as t1_t3_x, (t2.b + t3.b) as t2_t3_x
--from table1 as t1
--join table2 as t2 on t1_t2_x = t2.a
--join table3 as t3 on t1_t3_x = t2_t3_x;

DROP STREAM table1;
DROP STREAM table2;
DROP STREAM table3;
DROP STREAM table5;
