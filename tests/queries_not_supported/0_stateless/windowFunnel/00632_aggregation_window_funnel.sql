SET query_mode = 'table';
drop stream if exists funnel_test;

create stream funnel_test (timestamp uint32, event uint32) engine=Memory;
insert into funnel_test values (0,1000),(1,1001),(2,1002),(3,1003),(4,1004),(5,1005),(6,1006),(7,1007),(8,1008);

select 1 = windowFunnel(10000)(timestamp, event = 1000) from funnel_test;
select 2 = windowFunnel(10000)(timestamp, event = 1000, event = 1001) from funnel_test;
select 3 = windowFunnel(10000)(timestamp, event = 1000, event = 1001, event = 1002) from funnel_test;
select 4 = windowFunnel(10000)(timestamp, event = 1000, event = 1001, event = 1002, event = 1008) from funnel_test;


select 1 = windowFunnel(1)(timestamp, event = 1000) from funnel_test;
select 3 = windowFunnel(2)(timestamp, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test;
select 4 = windowFunnel(3)(timestamp, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test;
select 5 = windowFunnel(4)(timestamp, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test;


drop stream if exists funnel_test2;
create stream funnel_test2 (uid uint32 default 1,timestamp datetime, event uint32) engine=Memory;
insert into funnel_test2(timestamp, event) values  ('2018-01-01 01:01:01',1001),('2018-01-01 01:01:02',1002),('2018-01-01 01:01:03',1003),('2018-01-01 01:01:04',1004),('2018-01-01 01:01:05',1005),('2018-01-01 01:01:06',1006),('2018-01-01 01:01:07',1007),('2018-01-01 01:01:08',1008);


select 5 = windowFunnel(4)(timestamp, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test2;
select 2 = windowFunnel(10000)(timestamp, event = 1001, event = 1008) from funnel_test2;
select 1 = windowFunnel(10000)(timestamp, event = 1008, event = 1001) from funnel_test2;
select 5 = windowFunnel(4)(timestamp, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test2;
select 4 = windowFunnel(4)(timestamp, event <= 1007, event >= 1002, event <= 1006, event >= 1004) from funnel_test2;


drop stream if exists funnel_test_u64;
create stream funnel_test_u64 (uid uint32 default 1,timestamp uint64, event uint32) engine=Memory;
insert into funnel_test_u64(timestamp, event) values  ( 1e14 + 1 ,1001),(1e14 + 2,1002),(1e14 + 3,1003),(1e14 + 4,1004),(1e14 + 5,1005),(1e14 + 6,1006),(1e14 + 7,1007),(1e14 + 8,1008);


select 5 = windowFunnel(4)(timestamp, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test_u64;
select 2 = windowFunnel(10000)(timestamp, event = 1001, event = 1008) from funnel_test_u64;
select 1 = windowFunnel(10000)(timestamp, event = 1008, event = 1001) from funnel_test_u64;
select 5 = windowFunnel(4)(timestamp, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test_u64;
select 4 = windowFunnel(4)(timestamp, event <= 1007, event >= 1002, event <= 1006, event >= 1004) from funnel_test_u64;


drop stream if exists funnel_test_strict;
create stream funnel_test_strict (timestamp uint32, event uint32) engine=Memory;
insert into funnel_test_strict values (00,1000),(10,1001),(20,1002),(30,1003),(40,1004),(50,1005),(51,1005),(60,1006),(70,1007),(80,1008);

select 6 = windowFunnel(10000, 'strict_deduplication')(timestamp, event = 1000, event = 1001, event = 1002, event = 1003, event = 1004, event = 1005, event = 1006) from funnel_test_strict;
select 7 = windowFunnel(10000)(timestamp, event = 1000, event = 1001, event = 1002, event = 1003, event = 1004, event = 1005, event = 1006) from funnel_test_strict;


drop stream funnel_test;
drop stream funnel_test2;
drop stream funnel_test_u64;
drop stream funnel_test_strict;

drop stream if exists funnel_test_strict_order;
create stream funnel_test_strict_order (dt datetime, user int, event string) engine = MergeTree() partition by dt order by user;
insert into funnel_test_strict_order values (1, 1, 'a') (2, 1, 'b') (3, 1, 'c');
insert into funnel_test_strict_order values (1, 2, 'a') (2, 2, 'd') (3, 2, 'b') (4, 2, 'c');
insert into funnel_test_strict_order values (1, 3, 'a') (2, 3, 'a') (3, 3, 'b') (4, 3, 'b') (5, 3, 'c') (6, 3, 'c');
insert into funnel_test_strict_order values (1, 4, 'a') (2, 4, 'a') (3, 4, 'a') (4, 4, 'a') (5, 4, 'b') (6, 4, 'b') (7, 4, 'c') (8, 4, 'c');
insert into funnel_test_strict_order values (1, 5, 'a') (2, 5, 'a') (3, 5, 'b') (4, 5, 'b') (5, 5, 'd') (6, 5, 'c') (7, 5, 'c');
insert into funnel_test_strict_order values (1, 6, 'c') (2, 6, 'c') (3, 6, 'b') (4, 6, 'b') (5, 6, 'a') (6, 6, 'a');
select user, windowFunnel(86400)(dt, event='a', event='b', event='c') as s from funnel_test_strict_order group by user order by user format JSONCompactEachRow;
select user, windowFunnel(86400, 'strict_order')(dt, event='a', event='b', event='c') as s from funnel_test_strict_order group by user order by user format JSONCompactEachRow;
select user, windowFunnel(86400, 'strict_deduplication', 'strict_order')(dt, event='a', event='b', event='c') as s from funnel_test_strict_order group by user order by user format JSONCompactEachRow;
insert into funnel_test_strict_order values (1, 7, 'a') (2, 7, 'c') (3, 7, 'b');
select user, windowFunnel(10, 'strict_order')(dt, event = 'a', event = 'b', event = 'c') as s from funnel_test_strict_order where user = 7 group by user format JSONCompactEachRow;
drop stream funnel_test_strict_order;

--https://github.com/ClickHouse/ClickHouse/issues/27469
drop stream if exists strict_BiteTheDDDD;
create stream strict_BiteTheDDDD (ts uint64, event string) engine = Log();
insert into strict_BiteTheDDDD values (1,'a') (2,'b') (3,'c') (4,'b') (5,'d');
select 3 = windowFunnel(86400, 'strict_deduplication')(ts, event='a', event='b', event='c', event='d') from strict_BiteTheDDDD format JSONCompactEachRow;
drop stream strict_BiteTheDDDD;

drop stream if exists funnel_test_non_null;
create stream funnel_test_non_null (`dt` datetime, `u` int, `a` nullable(string), `b` nullable(string)) engine = MergeTree() partition by dt order by u;
insert into funnel_test_non_null values (1, 1, 'a1', 'b1') (2, 1, 'a2', 'b2');
insert into funnel_test_non_null values (1, 2, 'a1', null) (2, 2, 'a2', null);
insert into funnel_test_non_null values (1, 3, null, null);
insert into funnel_test_non_null values (1, 4, null, 'b1') (2, 4, 'a2', null) (3, 4, null, 'b3');
select u, windowFunnel(86400)(dt, COALESCE(a, '') = 'a1', COALESCE(a, '') = 'a2') as s from funnel_test_non_null group by u order by u format JSONCompactEachRow;
select u, windowFunnel(86400)(dt, COALESCE(a, '') = 'a1', COALESCE(b, '') = 'b2') as s from funnel_test_non_null group by u order by u format JSONCompactEachRow;
select u, windowFunnel(86400)(dt, a is null and b is null) as s from funnel_test_non_null group by u order by u format JSONCompactEachRow;
select u, windowFunnel(86400)(dt, a is null, COALESCE(b, '') = 'b3') as s from funnel_test_non_null group by u order by u format JSONCompactEachRow;
select u, windowFunnel(86400, 'strict_order')(dt, a is null, COALESCE(b, '') = 'b3') as s from funnel_test_non_null group by u order by u format JSONCompactEachRow;
drop stream funnel_test_non_null;

create stream funnel_test_strict_increase (timestamp uint32, event uint32) engine=Memory;
insert into funnel_test_strict_increase values (0,1000),(1,1001),(1,1002),(1,1003),(2,1004);

select 5 = windowFunnel(10000)(timestamp, event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) from funnel_test_strict_increase;
select 2 = windowFunnel(10000, 'strict_increase')(timestamp, event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) from funnel_test_strict_increase;
select 3 = windowFunnel(10000)(timestamp, event = 1004, event = 1004, event = 1004) from funnel_test_strict_increase;
select 1 = windowFunnel(10000, 'strict_increase')(timestamp, event = 1004, event = 1004, event = 1004) from funnel_test_strict_increase;

drop stream funnel_test_strict_increase;
