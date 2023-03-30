DROP STREAM IF EXISTS requests;
CREATE STREAM requests (
    event_time DateTime,
    event_date Date MATERIALIZED to_date(event_time),
    event_tm DateTime ALIAS event_time
) ENGINE = MergeTree ORDER BY (event_time);

INSERT INTO requests (event_time) VALUES ('2010-01-01 00:00:00');

select * from requests where event_date > '2000-01-01';

select * from requests as t where t.event_date > '2000-01-01';
select * from requests as "t" where "t".event_date > '2000-01-01';

select * from requests as t where t.event_tm > to_date('2000-01-01');
select * from requests as `t` where `t`.event_tm > to_date('2000-01-01');

DROP STREAM requests;
