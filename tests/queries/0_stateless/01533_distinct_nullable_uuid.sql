DROP STREAM IF EXISTS bug_14144;

create stream bug_14144
( meta_source_req_uuid nullable(uuid),
  a int64,
  meta_source_type string
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO bug_14144 SELECT cast(to_uuid('442d3ff4-842a-45bb-8b02-b616122c0dc6'), 'nullable(uuid)'), number, 'missing' FROM numbers(1000);

INSERT INTO bug_14144 SELECT cast(toUUIDOrZero('2fc89389-4728-4b30-9e51-b5bc3ad215f6'), 'nullable(uuid)'), number, 'missing' FROM numbers(1000);

INSERT INTO bug_14144 SELECT cast(toUUIDOrNull('05fe40cb-1d0c-45b0-8e60-8e311c2463f1'), 'nullable(uuid)'), number, 'missing' FROM numbers(1000);

SELECT DISTINCT meta_source_req_uuid
FROM bug_14144
WHERE meta_source_type = 'missing'
ORDER BY meta_source_req_uuid ASC;

TRUNCATE TABLE bug_14144;

INSERT INTO bug_14144 SELECT generateUUIDv4(), number, 'missing' FROM numbers(10000);

SELECT count() FROM (
   SELECT DISTINCT meta_source_req_uuid
   FROM bug_14144
   WHERE meta_source_type = 'missing'
   ORDER BY meta_source_req_uuid ASC
   LIMIT 100000
);

DROP STREAM bug_14144;




