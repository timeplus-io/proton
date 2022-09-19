DROP STREAM IF EXISTS tmp_01375;
DROP STREAM IF EXISTS table_tsv_01375;

create stream tmp_01375 (n uint32, s string) ;
create stream table_tsv_01375 AS tmp_01375 ENGINE = File(TSVWithNames);

INSERT INTO table_tsv_01375 SELECT number as n, to_string(n) as s FROM numbers(10);
INSERT INTO table_tsv_01375 SELECT number as n, to_string(n) as s FROM numbers(10);
INSERT INTO table_tsv_01375 SELECT number as n, to_string(n) as s FROM numbers(10);

SELECT * FROM table_tsv_01375;

DROP STREAM IF EXISTS tmp_01375;
DROP STREAM IF EXISTS table_tsv_01375;
