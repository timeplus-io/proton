SET mutations_sync=2;

DROP STREAM IF EXISTS rep_data;
create stream rep_data
(
    p int,
    t DateTime,
    INDEX idx t TYPE minmax GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rep_data', '1')
PARTITION BY p
ORDER BY t
SETTINGS number_of_free_entries_in_pool_to_execute_mutation=0;
INSERT INTO rep_data VALUES (1, now());
ALTER STREAM rep_data MATERIALIZE INDEX idx IN PARTITION ID 'NO_SUCH_PART'; -- { serverError 248 }
ALTER STREAM rep_data MATERIALIZE INDEX idx IN PARTITION ID '1';
ALTER STREAM rep_data MATERIALIZE INDEX idx IN PARTITION ID '2';

DROP STREAM IF EXISTS data;
create stream data
(
    p int,
    t DateTime,
    INDEX idx t TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY t
SETTINGS number_of_free_entries_in_pool_to_execute_mutation=0;
INSERT INTO data VALUES (1, now());
ALTER STREAM data MATERIALIZE INDEX idx IN PARTITION ID 'NO_SUCH_PART'; -- { serverError 248 }
ALTER STREAM data MATERIALIZE INDEX idx IN PARTITION ID '1';
ALTER STREAM data MATERIALIZE INDEX idx IN PARTITION ID '2';
