SET check_query_single_value_result = 0;

DROP STREAM IF EXISTS check_codec;

CREATE STREAM check_codec(a int, b int CODEC(Delta, ZSTD)) ENGINE = MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO check_codec SELECT number, number * 2 FROM numbers(1000);
CHECK TABLE check_codec;

DROP STREAM check_codec;

CREATE STREAM check_codec(a int, b int CODEC(Delta, ZSTD)) ENGINE = MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part = '10M';
INSERT INTO check_codec SELECT number, number * 2 FROM numbers(1000);
CHECK TABLE check_codec;

DROP STREAM check_codec;
