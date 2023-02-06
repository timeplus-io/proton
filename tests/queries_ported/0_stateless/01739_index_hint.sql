-- { echo }

drop stream if exists tbl;

create stream tbl (p int64, t int64, f float64) Engine=MergeTree partition by p order by t settings index_granularity=1;

insert into tbl select number / 4, number, 0 from numbers(16);

select * from tbl WHERE index_hint(t = 1) order by t;

select * from tbl WHERE index_hint(t in (select to_int64(number) + 2 from numbers(3))) order by t;

select * from tbl WHERE index_hint(p = 2) order by t;

select * from tbl WHERE index_hint(p in (select to_int64(number) - 2 from numbers(3))) order by t;

drop stream tbl;

drop stream if exists XXXX;

create stream XXXX (t int64, f float64) Engine=MergeTree order by t settings index_granularity=128;

insert into XXXX select number*60, 0 from numbers(100000);

SELECT sum(t) FROM XXXX WHERE index_hint(t = 42);

drop stream if exists XXXX;

create stream XXXX (t int64, f float64) Engine=MergeTree order by t settings index_granularity=8192;

insert into XXXX select number*60, 0 from numbers(100000);

SELECT count() FROM XXXX WHERE index_hint(t = to_datetime(0));

drop stream XXXX;
