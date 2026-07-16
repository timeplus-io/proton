-- Validate the MergeTree setting `primary_key_lazy_load`.
-- When 1 (default) a part's primary key index is not loaded at ATTACH/startup;
-- it is loaded lazily on first use. When 0 it is loaded eagerly at load time.
-- Residency is observed via system.parts.primary_key_bytes_in_memory
-- (0 = not resident, > 0 = resident). Assert the boolean so the reference is
-- stable across exact byte counts.

-- lazy = 1 (default): index must NOT be resident right after ATTACH.
DROP STREAM IF EXISTS t_index_lazy_load;

CREATE STREAM t_index_lazy_load (a uint64)
Engine=MergeTree ORDER BY a
SETTINGS index_granularity = 4, index_granularity_bytes = '10M', primary_key_lazy_load = 1;

INSERT INTO t_index_lazy_load(a) SELECT number FROM numbers(15);

DETACH STREAM t_index_lazy_load;
ATTACH STREAM t_index_lazy_load;

-- Not requested yet -> not loaded.
SELECT 'lazy=1 after attach, resident>0', primary_key_bytes_in_memory > 0
FROM system.parts
WHERE database = current_database() AND table = 't_index_lazy_load';

-- A primary-key query forces getIndex(); the index must then be resident.
SELECT count() FROM t_index_lazy_load WHERE a >= 10 SETTINGS query_mode = 'table';

SELECT 'lazy=1 after pk query, resident>0', primary_key_bytes_in_memory > 0
FROM system.parts
WHERE database = current_database() AND table = 't_index_lazy_load';

DROP STREAM t_index_lazy_load;

-- lazy = 0: index must be resident eagerly, right after ATTACH.
DROP STREAM IF EXISTS t_index_eager_load;

CREATE STREAM t_index_eager_load (a uint64)
Engine=MergeTree ORDER BY a
SETTINGS index_granularity = 4, index_granularity_bytes = '10M', primary_key_lazy_load = 0;

INSERT INTO t_index_eager_load(a) SELECT number FROM numbers(15);

DETACH STREAM t_index_eager_load;
ATTACH STREAM t_index_eager_load;

SELECT 'lazy=0 after attach, resident>0', primary_key_bytes_in_memory > 0
FROM system.parts
WHERE database = current_database() AND table = 't_index_eager_load';

DROP STREAM t_index_eager_load;
