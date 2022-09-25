SET optimize_on_insert = 0;
SET query_mode = 'table';

drop stream IF EXISTS test_00616;
drop stream IF EXISTS replacing_00616;

create stream test_00616
(
    date date,
    x int32,
    ver uint64
)
ENGINE = MergeTree(date, x, 4096);

INSERT INTO test_00616 VALUES ('2018-03-21', 1, 1), ('2018-03-21', 1, 2);
create stream replacing_00616 ENGINE = ReplacingMergeTree(date, x, 4096, ver) AS SELECT * FROM test_00616;

SELECT * FROM test_00616 ORDER BY ver;

SELECT * FROM replacing_00616 ORDER BY ver;
SELECT * FROM replacing_00616 FINAL ORDER BY ver;

OPTIMIZE STREAM replacing_00616 PARTITION '201803' FINAL;

SELECT * FROM replacing_00616 ORDER BY ver;
SELECT * FROM replacing_00616 FINAL ORDER BY ver;

drop stream test_00616;
drop stream replacing_00616;
