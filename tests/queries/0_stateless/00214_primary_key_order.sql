DROP STREAM IF EXISTS primary_key;
create stream primary_key (d date DEFAULT today(), x int8) ENGINE = MergeTree(d, -x, 1);

INSERT INTO primary_key (x) VALUES (1), (2), (3);

SELECT x FROM primary_key ORDER BY x;

SELECT 'a', -x FROM primary_key WHERE -x < -3;
SELECT 'b', -x FROM primary_key WHERE -x < -2;
SELECT 'c', -x FROM primary_key WHERE -x < -1;
SELECT 'd', -x FROM primary_key WHERE -x < to_int8(0);

DROP STREAM primary_key;
