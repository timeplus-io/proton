DROP STREAM IF EXISTS table1;
DROP STREAM IF EXISTS table2;

create stream table1
(
dt date,
id int32,
arr array(LowCardinality(string))
) ENGINE = MergeTree PARTITION BY to_monday(dt)
ORDER BY (dt, id) SETTINGS index_granularity = 8192;

create stream table2
(
dt date,
id int32,
arr array(LowCardinality(string))
) ENGINE = MergeTree PARTITION BY to_monday(dt)
ORDER BY (dt, id) SETTINGS index_granularity = 8192;

insert into table1 (dt, id, arr) values ('2019-01-14', 1, ['aaa']);
insert into table2 (dt, id, arr) values ('2019-01-14', 1, ['aaa','bbb','ccc']);

select dt, id, arraySort(groupArrayArray(arr))
from (
    select dt, id, arr from table1
    where dt = '2019-01-14' and id = 1
    UNION ALL
    select dt, id, arr from table2
    where dt = '2019-01-14' and id = 1
)
group by dt, id;

DROP STREAM table1;
DROP STREAM table2;
