DROP STREAM IF EXISTS stream1;

create stream stream1 (
    col1 int32,
    col2 int32
)
ENGINE = MergeTree
partition by tuple()
order by col1;

INSERT INTO stream1 VALUES (1, 2), (1, 4);

with NULL as pid
select a.col1, sum(a.col2) as summ
from stream1 a
prewhere (pid is null or a.col2 = pid)
group by a.col1;

with 123 as pid
select a.col1, sum(a.col2) as summ
from stream1 a
prewhere (pid is null or a.col2 = pid)
group by a.col1;

DROP STREAM stream1;
