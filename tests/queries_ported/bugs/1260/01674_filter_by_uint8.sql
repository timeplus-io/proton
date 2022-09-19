-- ORDER BY is to trigger comparison at uninitialized memory after bad filtering.
SELECT ignore(number) FROM numbers(256) ORDER BY array_filter(x -> materialize(255), materialize([257])) LIMIT 1;
SELECT ignore(number) FROM numbers(256) ORDER BY array_filter(x -> materialize(255), materialize(['257'])) LIMIT 1;

SELECT count() FROM numbers(256) WHERE to_uint8(number);

DROP STREAM IF EXISTS t_filter;
create stream t_filter(s string, a array(FixedString(3)), u uint64, f uint8)
ENGINE = MergeTree ORDER BY u;

INSERT INTO t_filter SELECT to_string(number), ['foo', 'bar'], number, to_uint8(number) FROM numbers(1000);
SELECT * FROM t_filter WHERE f LIMIT 5;

DROP STREAM IF EXISTS t_filter;
