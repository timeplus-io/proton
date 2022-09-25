DROP STREAM IF EXISTS empty_pk;
create stream empty_pk (x uint64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 256;

INSERT INTO empty_pk SELECT number FROM numbers(100000);

SELECT sum(x) from empty_pk;

DROP STREAM empty_pk;
