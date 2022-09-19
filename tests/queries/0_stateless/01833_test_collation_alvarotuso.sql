-- Tags: no-fasttest

DROP STREAM IF EXISTS test_collation;

create stream test_collation
(
    `v` string,
    `v2` string
)
ENGINE = MergeTree
ORDER BY v
SETTINGS index_granularity = 8192;

insert into test_collation values ('A', 'A');
insert into test_collation values ('B', 'B');
insert into test_collation values ('C', 'C');
insert into test_collation values ('a', 'a');
insert into test_collation values ('b', 'b');
insert into test_collation values ('c', 'c');

SELECT * FROM test_collation ORDER BY v ASC COLLATE 'en';

DROP STREAM test_collation;
