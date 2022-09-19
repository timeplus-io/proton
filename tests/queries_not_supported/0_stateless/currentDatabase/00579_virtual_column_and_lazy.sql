DROP STREAM IF EXISTS sample_00579_1;
DROP STREAM IF EXISTS sample_00579_2;
DROP STREAM IF EXISTS sample_merge_00579;

create stream sample_00579_1 (x uint64, d date DEFAULT today()) ENGINE = MergeTree(d, intHash64(x), intHash64(x), 10);
create stream sample_00579_2 (x uint64, d date DEFAULT today()) ENGINE = MergeTree(d, intHash64(x), intHash64(x), 10);

INSERT INTO sample_00579_1 (x) SELECT number AS x FROM system.numbers LIMIT 1000;
INSERT INTO sample_00579_2 (x) SELECT number AS x FROM system.numbers LIMIT 2000;

create stream sample_merge_00579 AS sample_00579_1 ENGINE = Merge(currentDatabase(), '^sample_00579_\\d$');

SET max_threads = 1;
SELECT _sample_factor FROM merge(currentDatabase(), '^sample_00579_\\d$');

DROP STREAM sample_00579_1;
DROP STREAM sample_00579_2;
DROP STREAM sample_merge_00579;
