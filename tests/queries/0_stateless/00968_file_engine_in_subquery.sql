DROP STREAM IF EXISTS tableFile_00968;
DROP STREAM IF EXISTS tableMergeTree_00968;
create stream tableFile_00968(number uint64) ENGINE = File('TSV');
create stream tableMergeTree_00968(id uint64) ENGINE = MergeTree() PARTITION BY id ORDER BY id;

INSERT INTO tableFile_00968 SELECT number FROM system.numbers LIMIT 10;
INSERT INTO tableMergeTree_00968 SELECT number FROM system.numbers LIMIT 100;

SELECT id FROM tableMergeTree_00968 WHERE id IN (SELECT number FROM tableFile_00968) ORDER BY id;

DROP STREAM tableFile_00968;
DROP STREAM tableMergeTree_00968;
