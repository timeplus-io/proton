DROP STREAM IF EXISTS stripe1;
DROP STREAM IF EXISTS stripe2;
DROP STREAM IF EXISTS stripe3;
DROP STREAM IF EXISTS stripe4;
DROP STREAM IF EXISTS stripe5;
DROP STREAM IF EXISTS stripe6;
DROP STREAM IF EXISTS stripe7;
DROP STREAM IF EXISTS stripe8;
DROP STREAM IF EXISTS stripe9;
DROP STREAM IF EXISTS stripe10;
DROP STREAM IF EXISTS merge_00401;

create stream stripe1 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
create stream stripe2 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
create stream stripe3 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
create stream stripe4 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
create stream stripe5 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
create stream stripe6 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
create stream stripe7 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
create stream stripe8 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
create stream stripe9 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;
create stream stripe10 ENGINE = StripeLog AS SELECT number AS x FROM system.numbers LIMIT 10;

create stream merge_00401 AS stripe1 ENGINE = Merge(currentDatabase(), '^stripe\\d+');

SELECT x, count() FROM merge_00401 GROUP BY x ORDER BY x;
SET max_threads = 1;
SELECT x, count() FROM merge_00401 GROUP BY x ORDER BY x;
SET max_threads = 2;
SELECT x, count() FROM merge_00401 GROUP BY x ORDER BY x;
SET max_threads = 5;
SELECT x, count() FROM merge_00401 GROUP BY x ORDER BY x;
SET max_threads = 10;
SELECT x, count() FROM merge_00401 GROUP BY x ORDER BY x;
SET max_threads = 20;
SELECT x, count() FROM merge_00401 GROUP BY x ORDER BY x;

DROP STREAM IF EXISTS stripe1;
DROP STREAM IF EXISTS stripe2;
DROP STREAM IF EXISTS stripe3;
DROP STREAM IF EXISTS stripe4;
DROP STREAM IF EXISTS stripe5;
DROP STREAM IF EXISTS stripe6;
DROP STREAM IF EXISTS stripe7;
DROP STREAM IF EXISTS stripe8;
DROP STREAM IF EXISTS stripe9;
DROP STREAM IF EXISTS stripe10;
DROP STREAM IF EXISTS merge_00401;
