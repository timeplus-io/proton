create stream test
(
    dt date,
    id uint32,
    val Nullable(uint32)
)
ENGINE = MergeTree(dt, id, 8192);

insert into test (dt, id, val) values ('2017-01-01', 1, 10);
insert into test (dt, id, val) values ('2017-01-01', 1, null);
insert into test (dt, id, val) values ('2017-01-01', 1, 0);

SELECT count()
FROM test
WHERE val = 0;

DROP STREAM IF EXISTS test;
