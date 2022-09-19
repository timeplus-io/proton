DROP STREAM IF EXISTS mutation_table;

create stream mutation_table
(
    date date,
    key uint64,
    value string
)
ENGINE = MergeTree()
PARTITION BY date
ORDER BY tuple();

INSERT INTO mutation_table SELECT to_date('2019-10-01'), number, '42' FROM numbers(100);

INSERT INTO mutation_table SELECT to_date('2019-10-02'), number, 'Hello' FROM numbers(100);

SELECT distinct(value) FROM mutation_table ORDER BY value;

ALTER STREAM mutation_table MODIFY COLUMN value uint64 SETTINGS mutations_sync = 2; --{serverError 341}

SELECT distinct(value) FROM mutation_table ORDER BY value; --{serverError 6}

KILL MUTATION where table = 'mutation_table' and database = currentDatabase();

ALTER STREAM mutation_table MODIFY COLUMN value string SETTINGS mutations_sync = 2;

SELECT distinct(value) FROM mutation_table ORDER BY value;

DROP STREAM IF EXISTS mutation_table;
