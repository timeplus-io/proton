DROP STREAM IF EXISTS mv;
DROP STREAM IF EXISTS mv_source;
DROP STREAM IF EXISTS mv_target;

create stream mv_source (`a` uint64) ENGINE = MergeTree ORDER BY tuple();
create stream mv_target (`a` uint64) ENGINE = MergeTree ORDER BY tuple();

CREATE MATERIALIZED VIEW mv TO mv_target AS SELECT * FROM mv_source;

INSERT INTO mv_source VALUES (1);

ALTER STREAM mv_target ADD COLUMN b uint8;
INSERT INTO mv_source VALUES (1),(2),(3);

SELECT * FROM mv ORDER BY a;
SELECT * FROM mv_target ORDER BY a;

DROP STREAM mv;
DROP STREAM mv_source;
DROP STREAM mv_target;
