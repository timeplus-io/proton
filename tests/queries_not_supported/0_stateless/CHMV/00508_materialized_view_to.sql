-- Tags: no-parallel

DROP DATABASE IF EXISTS test_00508;
CREATE DATABASE test_00508;

USE test_00508;

create stream src (x uint8) ENGINE = Null;
create stream dst (x uint8) ;

CREATE MATERIALIZED VIEW mv_00508 TO dst AS SELECT * FROM src;

INSERT INTO src VALUES (1), (2);
SELECT * FROM mv_00508 ORDER BY x;

-- Detach MV and see if the data is still readable
DETACH TABLE mv_00508;
SELECT * FROM dst ORDER BY x;

USE default;

-- Reattach MV (shortcut)
ATTACH TABLE test_00508.mv_00508;

INSERT INTO test_00508.src VALUES (3);

SELECT * FROM test_00508.mv_00508 ORDER BY x;

-- Drop the MV and see if the data is still readable
DROP STREAM test_00508.mv_00508;
SELECT * FROM test_00508.dst ORDER BY x;

DROP STREAM test_00508.src;
DROP STREAM test_00508.dst;

DROP DATABASE test_00508;
