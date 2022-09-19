-- Tags: long, replica

DROP STREAM IF EXISTS r1;
DROP STREAM IF EXISTS r2;

create stream r1 (x string) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/r', 'r1') ORDER BY x;
create stream r2 (x string) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/r', 'r2') ORDER BY x;

SYSTEM STOP REPLICATED SENDS r1;
SYSTEM STOP REPLICATED SENDS r2;

INSERT INTO r1 VALUES ('Hello, world');
SELECT * FROM r1;
SELECT * FROM r2;
INSERT INTO r2 VALUES ('Hello, world');
SELECT '---';
SELECT * FROM r1;
SELECT * FROM r2;

SYSTEM START REPLICATED SENDS r1;
SYSTEM START REPLICATED SENDS r2;
SYSTEM SYNC REPLICA r1;
SYSTEM SYNC REPLICA r2;

SELECT * FROM r1;
SELECT * FROM r2;

DROP STREAM r1;
DROP STREAM r2;
