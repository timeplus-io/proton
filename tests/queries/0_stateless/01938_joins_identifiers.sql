DROP STREAM IF EXISTS "/t0";
DROP STREAM IF EXISTS "/t1";

create stream "/t0" (a int64, b int64) engine = MergeTree() partition by a order by a;
create stream "/t1" (a int64, b int64) engine = MergeTree() partition by a order by a;

insert into "/t0" values (0, 0);
insert into "/t1" values (0, 1);

select * from "/t0" join "/t1" using a;

DROP STREAM "/t0";
DROP STREAM "/t1";
