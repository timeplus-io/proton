SELECT to_date(s) FROM (SELECT array_join(['2017-01-02', '2017-1-02', '2017-01-2', '2017-1-2', '2017/01/02', '2017/1/02', '2017/01/2', '2017/1/2', '2017-11-12']) AS s);

DROP STREAM IF EXISTS date;
create stream date (d date) ;

INSERT INTO date VALUES ('2017-01-02'), ('2017-1-02'), ('2017-01-2'), ('2017-1-2'), ('2017/01/02'), ('2017/1/02'), ('2017/01/2'), ('2017/1/2'), ('2017-11-12');
SELECT * FROM date;

INSERT INTO date FORMAT JSONEachRow {"d": "2017-01-02"}, {"d": "2017-1-02"}, {"d": "2017-01-2"}, {"d": "2017-1-2"}, {"d": "2017/01/02"}, {"d": "2017/1/02"}, {"d": "2017/01/2"}, {"d": "2017/1/2"}, {"d": "2017-11-12"};
SELECT * FROM date ORDER BY d;

DROP STREAM date;

WITH to_date('2000-01-01') + rand() % (30000) AS EventDate SELECT * FROM numbers(1000000) WHERE EventDate != to_date(concat(to_string(to_year(EventDate)), '-', to_string(to_month(EventDate)), '-', to_string(to_day_of_month(EventDate))));
