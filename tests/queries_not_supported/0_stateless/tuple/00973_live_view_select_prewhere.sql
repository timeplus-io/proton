-- Tags: no-replicated-database, no-parallel, no-fasttest

SET allow_experimental_live_view = 1;

DROP STREAM IF EXISTS lv;
DROP STREAM IF EXISTS lv2;
DROP STREAM IF EXISTS mt;

create stream mt (a int32) Engine=MergeTree order by tuple();
CREATE LIVE VIEW lv AS SELECT sum(a) AS sum_a FROM mt PREWHERE a > 1;
CREATE LIVE VIEW lv2 AS SELECT sum(number) AS sum_number FROM system.numbers PREWHERE number > 1;

INSERT INTO mt VALUES (1),(2),(3);

SELECT *,_version FROM lv;
SELECT *,_version FROM lv PREWHERE sum_a > 5; -- { serverError 182 }

INSERT INTO mt VALUES (1),(2),(3);

SELECT *,_version FROM lv;
SELECT *,_version FROM lv PREWHERE sum_a > 10; -- { serverError 182 }

SELECT *,_version FROM lv2; -- { serverError 182 }
SELECT *,_version FROM lv2 PREWHERE sum_number > 10; -- { serverError 182 }

DROP STREAM lv;
DROP STREAM lv2;
DROP STREAM mt;
