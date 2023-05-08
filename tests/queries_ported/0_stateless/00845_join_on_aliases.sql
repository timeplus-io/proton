SET query_mode='table';
SET asterisk_include_reserved_columns=false;
DROP STREAM IF EXISTS table1;
DROP STREAM IF EXISTS table2;

CREATE STREAM table1 (a uint32, b uint32) ENGINE = Memory;
CREATE STREAM table2 (a uint32, b uint32) ENGINE = Memory;

INSERT INTO table1(a,b) SELECT number, number FROM numbers(10);
INSERT INTO table2(a,b) SELECT number * 2, number * 20 FROM numbers(6);

select sleep(3);

select t1.a as t1_a, t2.a
from table1 as t1
join table2 as t2 on table1.a = table2.a and t1.a = table2.a and t1_a = table2.a;

select t1.a as t1_a, t2.a
from table1 as t1
join table2 as t2 on table1.a = t2.a and t1.a = t2.a and t1_a = t2.a;

select t1.a as t1_a, t2.a as t2_a
from table1 as t1
join table2 as t2 on table1.a = t2_a and t1.a = t2_a and t1_a = t2_a;

select t1.a as t1_a, t2.a
from table1 as t1
join table2 as t2 on table1.a = table2.a and t1.a = t2.a and t1_a = t2.a;

select t1.a as t1_a, t2.a as t2_a
from table1 as t1
join table2 as t2 on table1.a = table2.a and t1.a = t2.a and t1_a = t2_a;

select *
from table1 as t1
join table2 as t2 on t1_a = t2_a
where (table1.a as t1_a) > 4 and (table2.a as t2_a) > 2;

select t1.*, t2.*
from table1 as t1
join table2 as t2 on t1_a = t2_a
where (t1.a as t1_a) > 2 and (t2.a as t2_a) > 4;

DROP STREAM table1;
DROP STREAM table2;
