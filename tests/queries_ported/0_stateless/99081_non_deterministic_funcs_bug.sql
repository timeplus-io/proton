DROP STREAM IF EXISTS 99081_kv;

CREATE MUTABLE STREAM 99081_kv(i int, v string) primary key i;

select ts, ts2, v from (select now() as ts, i, v from 99081_kv) as kv1 join (select now64(3) as ts2, i from 99081_kv) as kv2 on kv1.i = kv2.i limit 0; -- { serverError UNSUPPORTED }
select ts, ts2, v from (select now() as ts, i, v from 99081_kv) as kv1 join (select now64(3) as ts2, i from 99081_kv) as kv2 on kv1.i = kv2.i limit 0 settings query_mode='table';

--- `now() as ts` and `now64(3) as ts2` are not used
select i, v from (select now() as ts, i, v from 99081_kv) as kv1 join (select now64(3) as ts2, i from 99081_kv) as kv2 on kv1.i = kv2.i limit 0;

select now() as ts, now64(3) as ts2, v, _tp_delta from (select i, v from 99081_kv) as kv1 join (select i from 99081_kv) as kv2 on kv1.i = kv2.i limit 0 emit changelog;
select now() as ts, now64(3) as ts2, v from (select i, v from 99081_kv) as kv1 join (select i from 99081_kv) as kv2 on kv1.i = kv2.i limit 0;

DROP STREAM IF EXISTS 99081_kv;
