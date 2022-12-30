DROP STREAM IF EXISTS distributed_tbl;
DROP STREAM IF EXISTS merge_tree_table;

create stream merge_tree_table
(
    date date,
    SomeType uint8,
    Alternative1 uint64,
    Alternative2 uint64,
    User uint32,
    CharID uint64 ALIAS multi_if(SomeType IN (3, 4, 11), 0, SomeType IN (7, 8), Alternative1, Alternative2)
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO merge_tree_table VALUES(to_date('2016-03-01'), 4, 0, 0, 1486392);

SELECT count() FROM merge_tree_table;

create stream distributed_tbl
(
    date date,
    SomeType uint8,
    Alternative1 uint64,
    Alternative2 uint64,
    CharID uint64,
    User uint32
)
ENGINE = Distributed(test_shard_localhost, currentDatabase(), merge_tree_table);

SELECT identity(CharID) AS x
FROM distributed_tbl
WHERE (date = to_date('2016-03-01')) AND (User = 1486392) AND (x = 0);

DROP STREAM IF EXISTS distributed_tbl;
DROP STREAM IF EXISTS merge_tree_table;
