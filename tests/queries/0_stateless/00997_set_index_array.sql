SET query_mode = 'table';
drop stream IF EXISTS set_array;

create stream set_array
(
    primary_key string,
    index_array array(uint64),
    INDEX additional_index_array (index_array) TYPE set(10000) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY (primary_key);

INSERT INTO set_array
select
  to_string(int_div(number, 1000000)) as primary_key,
  array(number) as index_array
from system.numbers
limit 10000000;

SET max_rows_to_read = 8192;

select count() from set_array where has(index_array, 333);

drop stream set_array;
