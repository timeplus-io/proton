drop  table if exists order_test1;

create stream order_test1
(
    timestamp DateTime64(3),
    color     low_cardinality(string)
) engine = MergeTree() ORDER BY tuple();

insert into order_test1 values ('2020-08-21 18:46:08.000','red')('2020-08-21 18:46:08.000','green');
insert into order_test1 values ('2020-08-21 18:46:07.000','red')('2020-08-21 18:46:07.000','green');
insert into order_test1 values ('2020-08-21 18:46:06.000','red')('2020-08-21 18:46:06.000','green');

SELECT color, to_datetime(timestamp) AS second
FROM order_test1
GROUP BY color, second
ORDER BY color ASC, second DESC;

select '';
select '';

SELECT  color, timestamp
FROM order_test1
GROUP BY color, timestamp
ORDER BY color ASC, timestamp DESC;

select '';
select '------cast to string----';
select '';

SELECT cast(color,'string') color, to_datetime(timestamp) AS second
FROM order_test1
GROUP BY color, second
ORDER BY color ASC, second DESC;

select '';
select '';

SELECT cast(color,'string') color, timestamp
FROM order_test1
GROUP BY color, timestamp
ORDER BY color ASC, timestamp DESC;

DROP STREAM order_test1;
