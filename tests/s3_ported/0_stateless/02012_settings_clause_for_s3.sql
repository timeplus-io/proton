-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

DROP STREAM IF EXISTS table_with_range;

CREATE STREAM table_with_range(`name` string,`number` uint32)　ENGINE = S3('http://localhost:11111/test/tsv_with_header.tsv', 'proton', 'protonproton', 'TSVWithNames')　SETTINGS input_format_with_names_use_header = 1;

select * from table_with_range;

DROP STREAM IF EXISTS table_with_range;
