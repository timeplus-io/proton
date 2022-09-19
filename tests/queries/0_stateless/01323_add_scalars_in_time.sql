SET optimize_on_insert = 0;

SET query_mode = 'table';
DROP STREAM IF EXISTS tags;

create stream tags (
    id string,
    seqs array(uint8),
    create_time DateTime DEFAULT now()
) engine=ReplacingMergeTree()
ORDER BY (id);

INSERT INTO tags(id, seqs) VALUES ('id1', [1,2,3]), ('id2', [0,2,3]), ('id1', [1,3]);

WITH
    (SELECT [0, 1, 2, 3]) AS arr1
SELECT arraySort(arrayIntersect(argMax(seqs, create_time), arr1)) AS common, id
FROM tags
WHERE id LIKE 'id%'
GROUP BY id;

DROP STREAM tags;


-- https://github.com/ClickHouse/ClickHouse/issues/15294

drop stream if exists TestTable;

create stream TestTable (column string, start DateTime, end DateTime) engine MergeTree order by start;

insert into TestTable (column, start, end) values('test', to_datetime('2020-07-20 09:00:00'), to_datetime('2020-07-20 20:00:00')),('test1', to_datetime('2020-07-20 09:00:00'), to_datetime('2020-07-20 20:00:00')),('test2', to_datetime('2020-07-20 09:00:00'), to_datetime('2020-07-20 20:00:00'));

SELECT column,
(SELECT d from (select [1, 2, 3, 4] as d)) as d
FROM TestTable
where column == 'test'
GROUP BY column;

drop stream TestTable;

-- https://github.com/ClickHouse/ClickHouse/issues/11407

drop stream if exists aaa;
drop stream if exists bbb;

create stream aaa (
    id uint16,
    data string
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY id;

INSERT INTO aaa VALUES (1, 'sef'),(2, 'fre'),(3, 'jhg');

create stream bbb (
    id uint16,
    data string
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY id;

INSERT INTO bbb VALUES (2, 'fre'), (3, 'jhg');

with (select group_array(id) from bbb) as ids
select *
  from aaa
 where has(ids, id)
order by id;


drop stream aaa;
drop stream bbb;
