DROP STREAM IF EXISTS table1;
DROP STREAM IF EXISTS table2;

create stream table1 (A string, B string, ts datetime) ENGINE = MergeTree PARTITION BY to_start_of_day(ts)  ORDER BY (ts, A, B);
create stream table2 (B string, ts datetime) ENGINE = MergeTree PARTITION BY to_start_of_day(ts) ORDER BY (ts, B);

insert into table1 values('a1','b1','2019-02-05 16:50:00'),('a1','b1','2019-02-05 16:55:00');
insert into table2 values('b1','2019-02-05 16:50:00'),('b1','2019-02-05 16:55:00');

SELECT t1.B, t2.B FROM table1 t1 ALL INNER JOIN table2 t2 ON t1.B = t2.B ORDER BY t1.B, t2.B;

DROP STREAM table1;
DROP STREAM table2;
