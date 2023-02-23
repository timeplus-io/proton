DROP STREAM IF EXISTS 02481_mergetree;
DROP STREAM IF EXISTS 02481_merge;

CREATE STREAM 02481_mergetree(x uint64, y uint64, arr array(string)) ENGINE = MergeTree ORDER BY x SAMPLE BY x;

CREATE STREAM 02481_merge(x uint64, y uint64, arr array(string)) ENGINE = Merge(current_database(), '^(02481_mergetree)$');

INSERT INTO 02481_mergetree SELECT number, number + 1, [1,2] FROM system.numbers LIMIT 100000;

SELECT count() FROM 02481_mergetree SAMPLE 1 / 2 ARRAY JOIN arr WHERE x != 0;
SELECT count() FROM 02481_merge SAMPLE 1 / 2 ARRAY JOIN arr WHERE x != 0;

DROP STREAM 02481_mergetree;
DROP STREAM 02481_merge;
