-- Tags: global, no-parallel

CREATE DATABASE IF NOT EXISTS test_00857;
USE test_00857;
DROP STREAM IF EXISTS local_table;
DROP STREAM IF EXISTS other_table;

create stream local_table
(
    id int32,
    name string,
    ts DateTime,
    oth_id int32
) ENGINE = MergeTree() PARTITION BY toMonday(ts) ORDER BY (ts, id);

create stream other_table
(
    id int32,
    name string,
    ts DateTime,
    trd_id int32
) ENGINE = MergeTree() PARTITION BY toMonday(ts) ORDER BY (ts, id);

INSERT INTO local_table VALUES(1, 'One', now(), 100);
INSERT INTO local_table VALUES(2, 'Two', now(), 200);
INSERT INTO other_table VALUES(100, 'One Hundred', now(), 1000);
INSERT INTO other_table VALUES(200, 'Two Hundred', now(), 2000);

select t2.name from remote('127.0.0.2', currentDatabase(), 'local_table') as t1
left join test_00857.other_table as t2 -- FIXME: doesn't work properly on remote without explicit database prefix
on t1.oth_id = t2.id
order by t2.name;

select t2.name from other_table as t2
global right join remote('127.0.0.2', currentDatabase(), 'local_table') as t1
on t1.oth_id = t2.id
order by t2.name;

select t2.name from remote('127.0.0.2', currentDatabase(), 'local_table') as t1
global left join other_table as t2
on t1.oth_id = t2.id
order by t2.name;

select t2.name from remote('127.0.0.2', currentDatabase(), 'local_table') as t1
global left join other_table as t2
on t1.oth_id = t2.id
order by t2.name;

select other_table.name from remote('127.0.0.2', currentDatabase(), 'local_table') as t1
global left join other_table
on t1.oth_id = other_table.id
order by other_table.name;

select other_table.name from remote('127.0.0.2', currentDatabase(), 'local_table') as t1
global left join other_table as t2
on t1.oth_id = other_table.id
order by other_table.name;

DROP STREAM local_table;
DROP STREAM other_table;
DROP DATABASE test_00857;
