DROP STREAM IF EXISTS t;

CREATE OR REPLACE VIEW t (number uint64) AS SELECT number FROM system.numbers;
SHOW create stream t;

CREATE OR REPLACE VIEW t AS SELECT number+1 AS next_number FROM system.numbers;
SHOW create stream t;

DROP STREAM t;
