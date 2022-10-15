DROP STREAM IF EXISTS hits_none;
CREATE STREAM hits_none (Title string CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO hits_none SELECT Title FROM table(test.hits);

SET min_bytes_to_use_mmap_io = 1;
SELECT sum(length(Title)) FROM table(hits)_none;

DROP STREAM hits_none;
