DROP STREAM IF EXISTS stream1;
DROP STREAM IF EXISTS stream2;

CREATE STREAM stream1
(
dt Date,
id int32,
arr array(low_cardinality(string))
) ENGINE = MergeTree PARTITION BY to_monday(dt)
ORDER BY (dt, id) SETTINGS index_granularity = 8192;

CREATE STREAM stream2
(
dt Date,
id int32,
arr array(low_cardinality(string))
) ENGINE = MergeTree PARTITION BY to_monday(dt)
ORDER BY (dt, id) SETTINGS index_granularity = 8192;

insert into stream1 (dt, id, arr) values ('2019-01-14', 1, ['aaa']);
insert into stream2 (dt, id, arr) values ('2019-01-14', 1, ['aaa','bbb','ccc']);

select dt, id, array_sort(group_array_array(arr))
from (
    select dt, id, arr from stream1
    where dt = '2019-01-14' and id = 1
    UNION ALL
    select dt, id, arr from stream2
    where dt = '2019-01-14' and id = 1
)
group by dt, id;

DROP STREAM stream1;
DROP STREAM stream2;
