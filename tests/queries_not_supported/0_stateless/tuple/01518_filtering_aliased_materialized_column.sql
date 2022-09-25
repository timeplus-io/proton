DROP STREAM IF EXISTS logs;

create stream logs( 
  date_visited datetime, 
  date date MATERIALIZED to_date(date_visited)
) ENGINE = MergeTree() ORDER BY tuple();

SELECT count() FROM logs AS plogs WHERE plogs.date = '2019-11-20';

INSERT INTO logs VALUES('2019-11-20 00:00:00');

SELECT count() FROM logs AS plogs WHERE plogs.date = '2019-11-20';

DROP STREAM logs;
