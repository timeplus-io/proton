DROP STREAM IF EXISTS test_01073_crlf_end_of_line;
create stream test_01073_crlf_end_of_line (value uint8, word string) ENGINE = MergeTree() ORDER BY value;
INSERT INTO test_01073_crlf_end_of_line VALUES (1, 'hello'), (2, 'world');
SELECT * FROM test_01073_crlf_end_of_line FORMAT CSV SETTINGS output_format_csv_crlf_end_of_line = 1;
SELECT * FROM test_01073_crlf_end_of_line FORMAT CSV SETTINGS output_format_csv_crlf_end_of_line = 0;
SELECT * FROM test_01073_crlf_end_of_line FORMAT TSV SETTINGS output_format_tsv_crlf_end_of_line = 1;
SELECT * FROM test_01073_crlf_end_of_line FORMAT TSV SETTINGS output_format_tsv_crlf_end_of_line = 0;
DROP STREAM IF EXISTS test_01073_crlf_end_of_line;
