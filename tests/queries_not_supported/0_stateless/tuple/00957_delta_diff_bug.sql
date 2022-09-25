SET allow_suspicious_codecs = 1;

DROP STREAM IF EXISTS segfault_table;

create stream segfault_table (id uint16 CODEC(Delta(2))) ENGINE MergeTree() order by tuple();

INSERT INTO segfault_table VALUES (1111), (2222);

SELECT * FROM segfault_table;

DROP STREAM IF EXISTS segfault_table;
