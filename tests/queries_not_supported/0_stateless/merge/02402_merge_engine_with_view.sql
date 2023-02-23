-- #40014
CREATE STREAM m0 (id uint64) ENGINE=MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO m0 SELECT number FROM numbers(10);
CREATE STREAM m1 (id uint64, s string) ENGINE=MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO m1 SELECT number, 'boo' FROM numbers(10);
CREATE VIEW m1v AS SELECT id FROM m1;

CREATE STREAM m2 (id uint64) ENGINE=Merge(current_database(),'m0|m1v');

SELECT * FROM m2 WHERE id > 1 AND id < 5 ORDER BY id SETTINGS force_primary_key=1, max_bytes_to_read=64;

-- #40706
CREATE VIEW v AS SELECT 1;
SELECT 1 FROM merge(current_database(), '^v$');