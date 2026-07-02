-- Tags: long
--
-- Ported (simplified) from upstream ClickHouse 00443_optimize_final_vertical_merge.sh.
-- Asserts the lazy-DEFAULT semantic for ALTER ADD COLUMN without DEFAULT
-- (upstream PR #88860, reference `[['def']] [['zzz']]`). The original upstream
-- test has additional scope (Replicated half + bash wrapper) that proton omits.

DROP STREAM IF EXISTS optimize_me_finally;

CREATE STREAM optimize_me_finally (
    date date,
    Sign int8,
    ki uint64,
    ds string,
    di01 uint64,
    di02 uint64,
    di03 uint64,
    di04 uint64,
    di05 uint64,
    di06 uint64,
    di07 uint64,
    di08 uint64,
    di09 uint64,
    di10 uint64,
    `n.i` array(uint64),
    `n.s` array(string)
)
ENGINE = CollapsingMergeTree(Sign)
PARTITION BY date
ORDER BY (date, ki)
SETTINGS
    index_granularity = 8192,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    min_bytes_for_wide_part = 0;

INSERT INTO optimize_me_finally (date, Sign, ki)
    SELECT to_date(0) AS date, to_int8(1) AS Sign, to_uint64(0) AS ki
    FROM system.numbers LIMIT 9000;

INSERT INTO optimize_me_finally (date, Sign, ki)
    SELECT to_date(0) AS date, to_int8(1) AS Sign, number AS ki
    FROM system.numbers LIMIT 9000, 9000;

INSERT INTO optimize_me_finally
    SELECT to_date(0) AS date,
           to_int8(1) AS Sign,
           number AS ki,
           hex(number) AS ds,
           number AS di01, number AS di02, number AS di03, number AS di04,
           number AS di05, number AS di06, number AS di07, number AS di08,
           number AS di09, number AS di10,
           [number, number + 1] AS `n.i`,
           [hex(number), hex(number + 1)] AS `n.s`
    FROM system.numbers LIMIT 150000;

OPTIMIZE STREAM optimize_me_finally FINAL;

ALTER STREAM optimize_me_finally ADD COLUMN `n.a` array(string);
ALTER STREAM optimize_me_finally ADD COLUMN da array(string) DEFAULT ['def'];

OPTIMIZE STREAM optimize_me_finally FINAL;

ALTER STREAM optimize_me_finally MODIFY COLUMN `n.a` array(string) DEFAULT ['zzz'];
ALTER STREAM optimize_me_finally MODIFY COLUMN da array(string) DEFAULT ['zzz'];

SELECT count(),
       sum(Sign),
       sum_if(1, ki = di05),
       sum_if(1, hex(ki) = ds),
       sum_if(1, ki = `n.i`[1]),
       sum_if(1, [hex(ki), hex(ki + 1)] = `n.s`)
FROM optimize_me_finally;

SELECT group_uniq_array(da), group_uniq_array(`n.a`) FROM optimize_me_finally;

DROP STREAM optimize_me_finally;
