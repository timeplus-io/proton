SET query_mode = 'table';
drop stream if exists t1;
drop stream if exists t2;

create stream t1
(
    col uint64,
    x uint64 MATERIALIZED col + 1
) Engine = MergeTree order by tuple();

create stream t2
(
    x uint64
) Engine = MergeTree order by tuple();

insert into t1 values (1),(2),(3),(4),(5);
insert into t2 values (1),(2),(3),(4),(5);

SELECT COUNT() FROM t1 INNER JOIN t2 USING x;

drop stream t1;
drop stream t2;
