DROP STREAM IF EXISTS ttl_empty_parts;

create stream ttl_empty_parts (id uint32, d date) ENGINE = MergeTree ORDER BY tuple() PARTITION BY id;

INSERT INTO ttl_empty_parts SELECT 0, to_date('2005-01-01') + number from numbers(500);
INSERT INTO ttl_empty_parts SELECT 1, to_date('2050-01-01') + number from numbers(500);

SELECT count() FROM ttl_empty_parts;
SELECT count() FROM system.parts WHERE table = 'ttl_empty_parts' AND database = currentDatabase() AND active;

ALTER STREAM ttl_empty_parts MODIFY TTL d;

-- To be sure, that task, which clears outdated parts executed.
DETACH STREAM ttl_empty_parts;
ATTACH STREAM ttl_empty_parts;

SELECT count() FROM ttl_empty_parts;
SELECT count() FROM system.parts WHERE table = 'ttl_empty_parts' AND database = currentDatabase() AND active;

DROP STREAM ttl_empty_parts;
