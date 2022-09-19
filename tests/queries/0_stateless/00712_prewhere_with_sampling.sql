SET query_mode = 'table';
drop stream if exists tab_00712_2;
create stream tab_00712_2 (a uint32, b uint32) engine = MergeTree order by b % 2 sample by b % 2;
insert into tab_00712_2 values (1, 2), (1, 4);
select a from tab_00712_2 sample 1 / 2 prewhere b = 2;
drop stream if exists tab_00712_2;

DROP STREAM IF EXISTS sample_prewhere;
create stream sample_prewhere (CounterID uint32, UserID uint64) ENGINE = MergeTree ORDER BY UserID SAMPLE BY UserID;
SELECT count() FROM sample_prewhere SAMPLE 1/2 PREWHERE CounterID = 1;
DROP STREAM sample_prewhere;
