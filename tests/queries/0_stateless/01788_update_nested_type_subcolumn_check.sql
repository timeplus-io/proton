DROP STREAM IF EXISTS test_wide_nested;

create stream test_wide_nested
(
    `id` int,
    `info.id` array(int),
    `info.name` array(string),
    `info.age` array(int)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

SELECT '********* test 1 **********';
set mutations_sync = 1;

INSERT INTO test_wide_nested SELECT number, [number,number + 1], ['aa','bb'], [number,number * 2] FROM numbers(5);

alter stream test_wide_nested update `info.id` = [100,200] where id = 1;
select * from test_wide_nested where id = 1 order by id;

alter stream test_wide_nested update `info.id` = [100,200,300], `info.age` = [10,20,30], `info.name` = ['a','b','c'] where id = 2;
select * from test_wide_nested;

alter stream test_wide_nested update `info.id` = [100,200,300], `info.age` = `info.id`, `info.name` = ['a','b','c'] where id = 2;
select * from test_wide_nested;

alter stream test_wide_nested update `info.id` = [100,200], `info.age`=[68,72] where id = 3;
alter stream test_wide_nested update `info.id` = `info.age` where id = 3;
select * from test_wide_nested;

alter stream test_wide_nested update `info.id` = [100,200], `info.age` = [10,20,30], `info.name` = ['a','b','c']  where id = 0; -- { serverError 341 }

-- Recreate table, because KILL MUTATION is not suitable for parallel tests execution.
SELECT '********* test 2 **********';
DROP STREAM test_wide_nested;

create stream test_wide_nested
(
    `id` int,
    `info.id` array(int),
    `info.name` array(string),
    `info.age` array(int)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO test_wide_nested SELECT number, [number,number + 1], ['aa','bb'], [number,number * 2] FROM numbers(5);
ALTER STREAM test_wide_nested ADD COLUMN `info2.id` array(int);
ALTER STREAM test_wide_nested ADD COLUMN `info2.name` array(string);
ALTER table test_wide_nested update `info2.id` = `info.id`, `info2.name` = `info.name` where 1;
select * from test_wide_nested;

alter stream test_wide_nested update `info.id` = [100,200,300], `info.age` = [10,20,30] where id = 1; -- { serverError 341 }

DROP STREAM test_wide_nested;

SELECT '********* test 3 **********';
DROP STREAM IF EXISTS test_wide_not_nested;

create stream test_wide_not_nested
(
  `id` int,
  `info.id` int,
  `info.name` string,
  `info.age` int
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO test_wide_not_nested SELECT number, number, 'aa', number * 2 FROM numbers(5);
ALTER STREAM test_wide_not_nested UPDATE `info.name` = 'bb' WHERE id = 1;
SELECT * FROM test_wide_not_nested ORDER BY id;

DROP STREAM test_wide_not_nested;
