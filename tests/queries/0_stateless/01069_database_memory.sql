-- Tags: no-parallel

DROP DATABASE IF EXISTS memory_01069;
CREATE DATABASE memory_01069 ;
SHOW CREATE DATABASE memory_01069;

create stream memory_01069.mt (n uint8) ENGINE = MergeTree() ORDER BY n;
create stream memory_01069.file (n uint8) ENGINE = File(CSV);

INSERT INTO memory_01069.mt VALUES (1), (2);
INSERT INTO memory_01069.file VALUES (3), (4);

SELECT * FROM memory_01069.mt ORDER BY n;
SELECT * FROM memory_01069.file ORDER BY n;

DROP STREAM memory_01069.mt;
SELECT * FROM memory_01069.mt ORDER BY n; -- { serverError 60 }
SELECT * FROM memory_01069.file ORDER BY n;

SHOW create stream memory_01069.mt; -- { serverError 60 }
SHOW create stream memory_01069.file;

DROP DATABASE memory_01069;
