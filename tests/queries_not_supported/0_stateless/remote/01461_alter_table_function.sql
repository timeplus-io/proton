DROP STREAM IF EXISTS table_from_remote;
DROP STREAM IF EXISTS table_from_select;
DROP STREAM IF EXISTS table_from_numbers;

create stream table_from_remote AS remote('localhost', 'system', 'numbers');

SHOW create stream table_from_remote;

ALTER STREAM table_from_remote ADD COLUMN col uint8;

SHOW create stream table_from_remote;

create stream table_from_numbers AS numbers(1000);

SHOW create stream table_from_numbers;

ALTER STREAM table_from_numbers ADD COLUMN col uint8; --{serverError 48}

SHOW create stream table_from_numbers;

create stream table_from_select ENGINE = MergeTree() ORDER BY tuple() AS SELECT number from system.numbers LIMIT 1;

SHOW create stream table_from_select;

ALTER STREAM table_from_select ADD COLUMN col uint8;

SHOW create stream table_from_select;

DROP STREAM IF EXISTS table_from_remote;
DROP STREAM IF EXISTS table_from_select;
DROP STREAM IF EXISTS table_from_numbers;
