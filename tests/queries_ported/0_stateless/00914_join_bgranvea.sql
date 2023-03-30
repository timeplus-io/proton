DROP STREAM IF EXISTS stream1;
DROP STREAM IF EXISTS stream2;

CREATE STREAM stream1 (A string, B string, ts DateTime) ENGINE = MergeTree PARTITION BY to_start_of_day(ts)  order by (ts, A, B);
CREATE STREAM stream2 (B string, ts DateTime) ENGINE = MergeTree PARTITION BY to_start_of_day(ts) order by (ts, B);

insert into stream1 values('a1','b1','2019-02-05 16:50:00'),('a1','b1','2019-02-05 16:55:00');
insert into stream2 values('b1','2019-02-05 16:50:00'),('b1','2019-02-05 16:55:00');

SELECT t1.B, t2.B FROM stream1 t1 ALL INNER JOIN stream2 t2 ON t1.B = t2.B ORDER BY t1.B, t2.B;

DROP STREAM stream1;
DROP STREAM stream2;
