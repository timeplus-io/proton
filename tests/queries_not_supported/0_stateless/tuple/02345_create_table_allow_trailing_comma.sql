DROP STREAM IF EXISTS trailing_comma_1 SYNC;
CREATE STREAM trailing_comma_1 (id INT NOT NULL DEFAULT 1,) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE STREAM trailing_comma_1;
DROP STREAM trailing_comma_1;

DROP STREAM IF EXISTS trailing_comma_2 SYNC;
CREATE STREAM trailing_comma_2 (id INT DEFAULT 1,) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE STREAM trailing_comma_2;
DROP STREAM trailing_comma_2;

DROP STREAM IF EXISTS trailing_comma_3 SYNC;
CREATE STREAM trailing_comma_3 (x uint8, y uint8,) ENGINE=MergeTree() ORDER BY tuple();
DESCRIBE STREAM trailing_comma_3;
DROP STREAM trailing_comma_3;
