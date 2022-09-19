SET query_mode = 'table';
drop stream IF EXISTS table_with_pk_clear;

create stream table_with_pk_clear(
  key1 uint64,
  key2 string,
  value1 string,
  value2 string
)
ENGINE = MergeTree()
ORDER by (key1, key2);

INSERT INTO table_with_pk_clear SELECT number, number * number, to_string(number), to_string(number * number) FROM numbers(1000);

ALTER STREAM table_with_pk_clear CLEAR COLUMN key1 IN PARTITION tuple(); --{serverError 524}

SELECT count(distinct key1) FROM table_with_pk_clear;

ALTER STREAM table_with_pk_clear CLEAR COLUMN key2 IN PARTITION tuple(); --{serverError 524}

SELECT count(distinct key2) FROM table_with_pk_clear;

drop stream IF EXISTS table_with_pk_clear;
