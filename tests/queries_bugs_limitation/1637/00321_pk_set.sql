SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;


DROP STREAM IF EXISTS pk_set;

create stream pk_set (d date, n uint64, host string, code uint64) ENGINE = MergeTree(d, (n, host, code), 1);
INSERT INTO pk_set (n, host, code) VALUES (1, 'market', 100), (11, 'news', 100);

SELECT count() FROM pk_set WHERE host IN ('admin.market1', 'admin.market2') AND code = 100;
SELECT count() FROM pk_set WHERE host IN ('admin.market1', 'admin.market2') AND code = 100 AND n = 11;
SELECT count() FROM pk_set WHERE host IN ('admin.market1', 'admin.market2') AND code = 100 AND n >= 11;
SELECT count() FROM pk_set WHERE host IN ('market', 'admin.market2', 'admin.market3', 'admin.market4', 'abc') AND code = 100 AND n = 11;
SELECT count() FROM pk_set WHERE host IN ('market', 'admin.market2', 'admin.market3', 'admin.market4', 'abc') AND code = 100 AND n >= 11;
SELECT count() FROM pk_set WHERE host IN ('admin.market2', 'admin.market3', 'admin.market4', 'abc') AND code = 100 AND n = 11;
SELECT count() FROM pk_set WHERE host IN ('admin.market2', 'admin.market3', 'admin.market4', 'abc', 'news') AND code = 100 AND n = 11;

-- that barely reproduces the problem
-- better way:
-- for i in {1..1000}; do echo "SELECT count() FROM pk_set WHERE host IN ('a'"$(seq 1 $i | sed -r "s/.+/,'\\0'/")") AND code = 100 AND n = 11;"; done > queries.tsv
-- clickhouse-benchmark < queries.tsv

DROP STREAM pk_set;
