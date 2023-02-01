DROP STREAM IF EXISTS source_table;
CREATE STREAM source_table (x uint16) ENGINE = TinyLog;
INSERT INTO source_table SELECT * FROM system.numbers LIMIT 10;

DROP STREAM IF EXISTS dest_view;
CREATE VIEW dest_view (x uint64) AS SELECT * FROM source_table;

SELECT x, any(x) FROM dest_view GROUP BY x ORDER BY x;

DROP STREAM dest_view;
DROP STREAM source_table;
