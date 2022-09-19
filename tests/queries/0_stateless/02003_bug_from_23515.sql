SET query_mode = 'table';
drop stream if exists tab;
create stream tab (a LowCardinality(string), b LowCardinality(string)) engine = MergeTree partition by a order by tuple() settings min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

insert into tab values ('1', 'a'), ('2', 'b');
SELECT a = '1' FROM tab WHERE a = '1' and b='a';

-- Fuzzed
SELECT * FROM tab WHERE (a = '1') AND 0 AND (b = 'a');

drop stream if exists tab;
