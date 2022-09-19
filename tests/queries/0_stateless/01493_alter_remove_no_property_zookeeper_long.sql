-- Tags: long, zookeeper

DROP STREAM IF EXISTS no_prop_table;

create stream no_prop_table
(
    some_column uint64
)
ENGINE MergeTree()
ORDER BY tuple();

SHOW create stream no_prop_table;

-- just nothing happened
ALTER STREAM no_prop_table MODIFY COLUMN some_column REMOVE DEFAULT; --{serverError 36}
ALTER STREAM no_prop_table MODIFY COLUMN some_column REMOVE MATERIALIZED; --{serverError 36}
ALTER STREAM no_prop_table MODIFY COLUMN some_column REMOVE ALIAS; --{serverError 36}
ALTER STREAM no_prop_table MODIFY COLUMN some_column REMOVE CODEC; --{serverError 36}
ALTER STREAM no_prop_table MODIFY COLUMN some_column REMOVE COMMENT; --{serverError 36}
ALTER STREAM no_prop_table MODIFY COLUMN some_column REMOVE TTL; --{serverError 36}

ALTER STREAM no_prop_table REMOVE TTL; --{serverError 36}

SHOW create stream no_prop_table;

DROP STREAM IF EXISTS no_prop_table;

DROP STREAM IF EXISTS r_no_prop_table;

create stream r_no_prop_table
(
  some_column uint64
)
ENGINE ReplicatedMergeTree('/clickhouse/{database}/test/01493_r_no_prop_table', '1')
ORDER BY tuple();

SHOW create stream r_no_prop_table;

ALTER STREAM r_no_prop_table MODIFY COLUMN some_column REMOVE DEFAULT; --{serverError 36}
ALTER STREAM r_no_prop_table MODIFY COLUMN some_column REMOVE MATERIALIZED; --{serverError 36}
ALTER STREAM r_no_prop_table MODIFY COLUMN some_column REMOVE ALIAS; --{serverError 36}
ALTER STREAM r_no_prop_table MODIFY COLUMN some_column REMOVE CODEC; --{serverError 36}
ALTER STREAM r_no_prop_table MODIFY COLUMN some_column REMOVE COMMENT; --{serverError 36}
ALTER STREAM r_no_prop_table MODIFY COLUMN some_column REMOVE TTL; --{serverError 36}

ALTER STREAM r_no_prop_table REMOVE TTL;  --{serverError 36}

SHOW create stream r_no_prop_table;

ALTER STREAM r_no_prop_table MODIFY COLUMN some_column REMOVE ttl;  --{serverError 36}
ALTER STREAM r_no_prop_table remove TTL;  --{serverError 36}

DROP STREAM IF EXISTS r_no_prop_table;
