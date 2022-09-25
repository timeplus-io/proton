DROP STREAM IF EXISTS t1;

create stream t1
(
    date date, 
    s1 string,
    s2 string
) ENGINE = MergeTree() PARTITION BY toYYYYMMDD(date) ORDER BY (date, s1)
SETTINGS index_granularity = 8192;

insert into t1 (date, s1,s2) values(today()-1,'aaa','bbb');
alter stream t1 add column s3 string DEFAULT concat(s2,'_',s1);
insert into t1 (date, s1,s2) values(today(),'aaa2','bbb2');
select ignore(date), s3 from t1 where  s2='bbb';

DROP STREAM t1;
