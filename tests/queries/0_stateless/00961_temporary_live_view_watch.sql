-- Tags: no-replicated-database, no-parallel, no-fasttest

SET allow_experimental_live_view = 1;

DROP STREAM IF EXISTS lv;
DROP STREAM IF EXISTS mt;

create stream mt (a int32) Engine=MergeTree order by tuple();
CREATE LIVE VIEW lv WITH TIMEOUT AS SELECT sum(a) FROM mt;

WATCH lv LIMIT 0;

INSERT INTO mt VALUES (1),(2),(3);

WATCH lv LIMIT 0;

INSERT INTO mt VALUES (4),(5),(6);

WATCH lv LIMIT 0;

DROP STREAM lv;
DROP STREAM mt;
