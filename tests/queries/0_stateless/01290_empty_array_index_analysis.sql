SET query_mode = 'table';
drop stream if exists count_lc_test;

create stream count_lc_test
(
    `s` low_cardinality(string),
    `arr` array(low_cardinality(string)),
    `num` uint64
)
ENGINE = MergeTree
ORDER BY (s, arr);

INSERT INTO count_lc_test(num, arr) VALUES (1,[]),(2,['a']),(3,['a','b','c']),(4,['aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa']);

SELECT '--- not_empty';
select * from count_lc_test where not_empty(arr);
SELECT '--- empty';
select * from count_lc_test where empty(arr);
SELECT '--- = []';
select * from count_lc_test where arr = [];
SELECT '--- != []';
select * from count_lc_test where arr != [];
SELECT '--- > []';
select * from count_lc_test where arr > [];
SELECT '--- < []';
select * from count_lc_test where arr < [];
SELECT '--- >= []';
select * from count_lc_test where arr >= [];
SELECT '--- <= []';
select * from count_lc_test where arr <= [];
SELECT '---';

DROP STREAM count_lc_test;


drop stream if exists count_lc_test;

create stream count_lc_test
(
    `s` low_cardinality(string),
    `arr` array(string),
    `num` uint64
)
ENGINE = MergeTree
ORDER BY (s, arr);

INSERT INTO count_lc_test(num, arr) VALUES (1,[]),(2,['a']),(3,['a','b','c']),(4,['aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa']);

SELECT '--- not_empty';
select * from count_lc_test where not_empty(arr);
SELECT '--- empty';
select * from count_lc_test where empty(arr);
SELECT '--- = []';
select * from count_lc_test where arr = [];
SELECT '--- != []';
select * from count_lc_test where arr != [];
SELECT '--- > []';
select * from count_lc_test where arr > [];
SELECT '--- < []';
select * from count_lc_test where arr < [];
SELECT '--- >= []';
select * from count_lc_test where arr >= [];
SELECT '--- <= []';
select * from count_lc_test where arr <= [];
SELECT '---';

DROP STREAM count_lc_test;
