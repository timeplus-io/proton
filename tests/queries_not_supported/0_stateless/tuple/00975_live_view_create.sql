-- Tags: no-replicated-database, no-parallel, no-fasttest

SET allow_experimental_live_view = 1;

DROP STREAM IF EXISTS mt;

create stream mt (a int32) Engine=MergeTree order by tuple();
CREATE LIVE VIEW lv AS SELECT * FROM mt;

DROP STREAM lv;
DROP STREAM mt;
