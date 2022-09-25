SET query_mode = 'table';
drop stream if exists rate_test;

create stream rate_test (timestamp uint32, event uint32) engine=Memory;
insert into rate_test values (0,1000),(1,1001),(2,1002),(3,1003),(4,1004),(5,1005),(6,1006),(7,1007),(8,1008);

select 1.0 = boundingRatio(timestamp, event) from rate_test;

drop stream if exists rate_test2;
create stream rate_test2 (uid uint32 default 1,timestamp datetime, event uint32) engine=Memory;
insert into rate_test2(timestamp, event) values ('2018-01-01 01:01:01',1001),('2018-01-01 01:01:02',1002),('2018-01-01 01:01:03',1003),('2018-01-01 01:01:04',1004),('2018-01-01 01:01:05',1005),('2018-01-01 01:01:06',1006),('2018-01-01 01:01:07',1007),('2018-01-01 01:01:08',1008);

select 1.0 = boundingRatio(timestamp, event) from rate_test2;

drop stream rate_test;
drop stream rate_test2;


SELECT boundingRatio(number, number * 1.5) FROM numbers(10);
SELECT boundingRatio(1000 + number, number * 1.5) FROM numbers(10);
SELECT boundingRatio(1000 + number, number * 1.5 - 111) FROM numbers(10);
SELECT number % 10 AS k, boundingRatio(1000 + number, number * 1.5 - 111) FROM numbers(100) GROUP BY k WITH TOTALS ORDER BY k;

SELECT boundingRatio(1000 + number, number * 1.5 - 111) FROM numbers(2);
SELECT boundingRatio(1000 + number, number * 1.5 - 111) FROM numbers(1);
SELECT boundingRatio(1000 + number, number * 1.5 - 111) FROM numbers(1) WHERE 0;
SELECT boundingRatio(number, exp(number)) = exp(1) - 1 FROM numbers(2);
