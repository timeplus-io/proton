DROP STREAM IF EXISTS 03443_data;

CREATE STREAM 03443_data
(
    id int32,
    name string,
    INDEX idx_name name TYPE ngrambf_v1(1, 1024, 3, 0) GRANULARITY 1
)
ORDER BY id SETTINGS index_granularity = 1
AS
SELECT 1, 'John', now() UNION ALL
SELECT 2, 'Ksenia', now() UNION ALL
SELECT 3, 'Alice', now();

SET query_mode='table';
select sleep(3) FORMAT Null;

SELECT '-- Without index';
SELECT name FROM 03443_data WHERE match(name, 'J|XYZ') SETTINGS use_skip_indexes = 0;
SELECT name FROM 03443_data WHERE match(name, 'XYZ|J') SETTINGS use_skip_indexes = 0;
SELECT name FROM 03443_data WHERE match(name, '[J]|XYZ') SETTINGS use_skip_indexes = 0;
SELECT name FROM 03443_data WHERE match(name, 'XYZ|[J]') SETTINGS use_skip_indexes = 0;

SELECT '-- With index';
SELECT name FROM 03443_data WHERE match(name, 'J|XYZ') SETTINGS use_skip_indexes = 1;
SELECT name FROM 03443_data WHERE match(name, 'XYZ|J') SETTINGS use_skip_indexes = 1;
SELECT name FROM 03443_data WHERE match(name, '[J]|XYZ') SETTINGS use_skip_indexes = 1;
SELECT name FROM 03443_data WHERE match(name, 'XYZ|[J]') SETTINGS use_skip_indexes = 1;

SELECT '-- Assert selected granules';

-- SET parallel_replicas_local_plan = 1;

SELECT trim(leading ' ' from explain) FROM (EXPLAIN indexes=1 SELECT name FROM 03443_data WHERE match(name, 'J|XYZ')) WHERE explain LIKE '%Granules: %' SETTINGS use_skip_indexes = 1;
SELECT '';
SELECT trim(leading ' ' from explain) FROM (EXPLAIN indexes=1 SELECT name FROM 03443_data WHERE match(name, 'XYZ|J')) WHERE explain LIKE '%Granules: %' SETTINGS use_skip_indexes = 1;
SELECT '';
SELECT trim(leading ' ' from explain) FROM (EXPLAIN indexes=1 SELECT name FROM 03443_data WHERE match(name, '[J]|XYZ')) WHERE explain LIKE '%Granules: %' SETTINGS use_skip_indexes = 1;
SELECT '';
SELECT trim(leading ' ' from explain) FROM (EXPLAIN indexes=1 SELECT name FROM 03443_data WHERE match(name, 'XYZ|[J]')) WHERE explain LIKE '%Granules: %' SETTINGS use_skip_indexes = 1;

DROP STREAM 03443_data;
