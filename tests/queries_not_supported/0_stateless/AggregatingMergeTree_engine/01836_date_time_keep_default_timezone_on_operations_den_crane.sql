SELECT to_type_name(now());
SELECT to_type_name(now() - 1);
SELECT to_type_name(now('UTC') - 1);

SELECT to_type_name(now64(3));
SELECT to_type_name(now64(3) - 1);
SELECT to_type_name(toTimeZone(now64(3), 'UTC') - 1);

DROP STREAM IF EXISTS tt_null;
DROP STREAM IF EXISTS tt;
DROP STREAM IF EXISTS tt_mv;

create stream tt_null(p string) engine = Null;

create stream tt(p string,tmin aggregate_function(min, DateTime)) 
engine = AggregatingMergeTree  order by p;

create materialized view tt_mv to tt as 
select p, minState(now() - interval 30 minute) as tmin
from tt_null group by p;

insert into tt_null values('x');

DROP STREAM tt_null;
DROP STREAM tt;
DROP STREAM tt_mv;
