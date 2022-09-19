DROP STREAM IF EXISTS sample_00314_1;
DROP STREAM IF EXISTS sample_00314_2;
DROP STREAM IF EXISTS sample_merge_00314;

create stream sample_00314_1 (x uint64, d date DEFAULT today()) ENGINE = MergeTree(d, intHash64(x), intHash64(x), 10);
create stream sample_00314_2 (x uint64, d date DEFAULT today()) ENGINE = MergeTree(d, intHash64(x), intHash64(x), 10);

SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

INSERT INTO sample_00314_1 (x) SELECT number AS x FROM system.numbers LIMIT 1000000;
INSERT INTO sample_00314_2 (x) SELECT number AS x FROM system.numbers LIMIT 2000000;

create stream sample_merge_00314 AS sample_00314_1 ENGINE = Merge(currentDatabase(), '^sample_00314_\\d$');

SELECT abs(sum(_sample_factor) - 3000000) / 3000000 < 0.001 FROM sample_merge_00314 SAMPLE 100000;
SELECT abs(sum(_sample_factor) - 3000000) / 3000000 < 0.001 FROM merge(currentDatabase(), '^sample_00314_\\d$') SAMPLE 100000;

DROP STREAM sample_00314_1;
DROP STREAM sample_00314_2;
DROP STREAM sample_merge_00314;
