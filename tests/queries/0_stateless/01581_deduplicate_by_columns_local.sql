--- See also tests/queries/0_stateless/01581_deduplicate_by_columns_replicated.sql

--- local case

-- Just in case if previous tests run left some stuff behind.
DROP STREAM IF EXISTS source_data;

create stream source_data (
    pk int32, sk int32, val uint32, partition_key uint32 DEFAULT 1,
    PRIMARY KEY (pk)
) ENGINE=MergeTree
ORDER BY (pk, sk);

INSERT INTO source_data (pk, sk, val) VALUES (0, 0, 0), (0, 0, 0), (1, 1, 2), (1, 1, 3);

SELECT 'TOTAL rows', count() FROM source_data;

DROP STREAM IF EXISTS full_duplicates;
-- table with duplicates on MATERIALIZED columns
create stream full_duplicates  (
    pk int32, sk int32, val uint32, partition_key uint32, mat uint32 MATERIALIZED 12345, alias uint32 ALIAS 2,
    PRIMARY KEY (pk)
) ENGINE=MergeTree
PARTITION BY (partition_key + 1) -- ensure that column in expression is properly handled when deduplicating. See [1] below.
ORDER BY (pk, to_string(sk * 10)); -- silly order key to ensure that key column is checked even when it is a part of expression. See [1] below.

-- ERROR cases
OPTIMIZE TABLE full_duplicates DEDUPLICATE BY pk, sk, val, mat, alias; -- { serverError 16 } -- alias column is present
OPTIMIZE TABLE full_duplicates DEDUPLICATE BY sk, val; -- { serverError 8 } -- primary key column is missing
OPTIMIZE TABLE full_duplicates DEDUPLICATE BY * EXCEPT(pk, sk, val, mat, alias, partition_key); -- { serverError 51 } -- list is empty
OPTIMIZE TABLE full_duplicates DEDUPLICATE BY * EXCEPT(pk); -- { serverError 8 } -- primary key column is missing [1]
OPTIMIZE TABLE full_duplicates DEDUPLICATE BY * EXCEPT(sk); -- { serverError 8 } -- sorting key column is missing [1]
OPTIMIZE TABLE full_duplicates DEDUPLICATE BY * EXCEPT(partition_key); -- { serverError 8 } -- partitioning column is missing [1]

OPTIMIZE TABLE full_duplicates DEDUPLICATE BY; -- { clientError 62 } -- empty list is a syntax error
OPTIMIZE TABLE partial_duplicates DEDUPLICATE BY pk,sk,val,mat EXCEPT mat; -- { clientError 62 } -- invalid syntax
OPTIMIZE TABLE partial_duplicates DEDUPLICATE BY pk APPLY(pk + 1); -- { clientError 62 } -- APPLY column transformer is not supported
OPTIMIZE TABLE partial_duplicates DEDUPLICATE BY pk REPLACE(pk + 1); -- { clientError 62 } -- REPLACE column transformer is not supported

-- Valid cases
-- NOTE: here and below we need FINAL to force deduplication in such a small set of data in only 1 part.

SELECT 'OLD DEDUPLICATE';
INSERT INTO full_duplicates SELECT * FROM source_data;
OPTIMIZE TABLE full_duplicates FINAL DEDUPLICATE;
SELECT * FROM full_duplicates;
TRUNCATE full_duplicates;

SELECT 'DEDUPLICATE BY *';
INSERT INTO full_duplicates SELECT * FROM source_data;
OPTIMIZE TABLE full_duplicates FINAL DEDUPLICATE BY *;
SELECT * FROM full_duplicates;
TRUNCATE full_duplicates;

SELECT 'DEDUPLICATE BY * EXCEPT mat';
INSERT INTO full_duplicates SELECT * FROM source_data;
OPTIMIZE TABLE full_duplicates FINAL DEDUPLICATE BY * EXCEPT mat;
SELECT * FROM full_duplicates;
TRUNCATE full_duplicates;

SELECT 'DEDUPLICATE BY pk,sk,val,mat,partition_key';
INSERT INTO full_duplicates SELECT * FROM source_data;
OPTIMIZE TABLE full_duplicates FINAL DEDUPLICATE BY pk,sk,val,mat,partition_key;
SELECT * FROM full_duplicates;
TRUNCATE full_duplicates;

--DROP STREAM full_duplicates;

-- Now to the partial duplicates when MATERIALIZED column alway has unique value.
DROP STREAM IF EXISTS partial_duplicates;
create stream partial_duplicates  (
    pk int32, sk int32, val uint32, partition_key uint32 DEFAULT 1, mat uint32 MATERIALIZED rand(), alias uint32 ALIAS 2,
    PRIMARY KEY (pk)
) ENGINE=MergeTree
ORDER BY (pk, sk);

SELECT 'Can not remove full duplicates';

-- should not remove anything
SELECT 'OLD DEDUPLICATE';
INSERT INTO partial_duplicates SELECT * FROM source_data;
OPTIMIZE TABLE partial_duplicates FINAL DEDUPLICATE;
SELECT count() FROM partial_duplicates;
TRUNCATE partial_duplicates;

SELECT 'DEDUPLICATE BY pk,sk,val,mat';
INSERT INTO partial_duplicates SELECT * FROM source_data;
OPTIMIZE TABLE partial_duplicates FINAL DEDUPLICATE BY pk,sk,val,mat;
SELECT count() FROM partial_duplicates;
TRUNCATE partial_duplicates;

SELECT 'Remove partial duplicates';

SELECT 'DEDUPLICATE BY *'; -- all except MATERIALIZED columns, hence will reduce number of rows.
INSERT INTO partial_duplicates SELECT * FROM source_data;
OPTIMIZE TABLE partial_duplicates FINAL DEDUPLICATE BY *;
SELECT count() FROM partial_duplicates;
TRUNCATE partial_duplicates;

SELECT 'DEDUPLICATE BY * EXCEPT mat';
INSERT INTO partial_duplicates SELECT * FROM source_data;
OPTIMIZE TABLE partial_duplicates FINAL DEDUPLICATE BY * EXCEPT mat;
SELECT * FROM partial_duplicates;
TRUNCATE partial_duplicates;

SELECT 'DEDUPLICATE BY COLUMNS("*") EXCEPT mat';
INSERT INTO partial_duplicates SELECT * FROM source_data;
OPTIMIZE TABLE partial_duplicates FINAL DEDUPLICATE BY COLUMNS('.*') EXCEPT mat;
SELECT * FROM partial_duplicates;
TRUNCATE partial_duplicates;

SELECT 'DEDUPLICATE BY pk,sk';
INSERT INTO partial_duplicates SELECT * FROM source_data;
OPTIMIZE TABLE partial_duplicates FINAL DEDUPLICATE BY pk,sk;
SELECT * FROM partial_duplicates;
TRUNCATE partial_duplicates;

SELECT 'DEDUPLICATE BY COLUMNS(".*k")';
INSERT INTO partial_duplicates SELECT * FROM source_data;
OPTIMIZE TABLE partial_duplicates FINAL DEDUPLICATE BY COLUMNS('.*k');
SELECT * FROM partial_duplicates;
TRUNCATE partial_duplicates;

DROP STREAM full_duplicates;
DROP STREAM partial_duplicates;
DROP STREAM source_data;
