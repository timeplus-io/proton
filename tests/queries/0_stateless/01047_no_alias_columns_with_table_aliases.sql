DROP STREAM IF EXISTS requests;
create stream requests (
    event_time datetime,
    event_date date MATERIALIZED to_date(event_time),
    event_tm datetime ALIAS event_time
) ENGINE = MergeTree ORDER BY (event_time);

INSERT INTO requests (event_time) VALUES ('2010-01-01 00:00:00');

select * from requests where event_date > '2000-01-01';

select * from requests as t where t.event_date > '2000-01-01';
select * from requests as "t" where "t".event_date > '2000-01-01';

select * from requests as t where t.event_tm > to_date('2000-01-01');
select * from requests as `t` where `t`.event_tm > to_date('2000-01-01');

DROP STREAM requests;
