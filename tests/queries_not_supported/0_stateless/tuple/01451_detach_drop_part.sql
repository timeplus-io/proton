DROP STREAM IF EXISTS mt_01451;

create stream mt_01451 (v uint8) ENGINE = MergeTree() order by tuple();
SYSTEM STOP MERGES mt_01451;

INSERT INTO mt_01451 VALUES (0);
INSERT INTO mt_01451 VALUES (1);
INSERT INTO mt_01451 VALUES (2);

SELECT v FROM mt_01451 ORDER BY v;

ALTER STREAM mt_01451 DETACH PART 'all_100_100_0'; -- { serverError 232 }

ALTER STREAM mt_01451 DETACH PART 'all_2_2_0';

SELECT v FROM mt_01451 ORDER BY v;

SELECT name FROM system.detached_parts WHERE table = 'mt_01451' AND database = currentDatabase();

ALTER STREAM mt_01451 ATTACH PART 'all_2_2_0';

SELECT v FROM mt_01451 ORDER BY v;

SELECT name FROM system.detached_parts WHERE table = 'mt_01451' AND database = currentDatabase();

SELECT '-- drop part --';

ALTER STREAM mt_01451 DROP PART 'all_4_4_0';

ALTER STREAM mt_01451 ATTACH PART 'all_4_4_0'; -- { serverError 233 }

SELECT v FROM mt_01451 ORDER BY v;

SELECT '-- resume merges --';
SYSTEM START MERGES mt_01451;
OPTIMIZE STREAM mt_01451 FINAL;

SELECT v FROM mt_01451 ORDER BY v;

SELECT name FROM system.parts WHERE table = 'mt_01451' AND active AND database = currentDatabase();

DROP STREAM mt_01451;
