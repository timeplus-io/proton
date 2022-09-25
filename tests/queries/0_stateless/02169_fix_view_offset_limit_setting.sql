DROP STREAM IF EXISTS counter;
create stream counter (id uint64, createdAt datetime) ENGINE = MergeTree() ORDER BY id;
INSERT INTO counter SELECT number, now() FROM numbers(500);

DROP STREAM IF EXISTS vcounter;
CREATE VIEW vcounter AS SELECT int_div(id, 10) AS tens, max(createdAt) AS maxid FROM counter GROUP BY tens;

SELECT tens FROM vcounter ORDER BY tens ASC LIMIT 100 SETTINGS limit = 6, offset = 5;

SELECT tens FROM vcounter ORDER BY tens ASC LIMIT 100 SETTINGS limit = 6, offset = 0;
DROP STREAM vcounter;
DROP STREAM counter;
