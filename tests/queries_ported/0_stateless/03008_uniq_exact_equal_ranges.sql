DROP STREAM IF EXISTS t_uniq_exact;

CREATE STREAM t_uniq_exact (a uint64, b string, c uint64) ORDER BY a;

SET group_by_two_level_threshold_bytes = 1;
SET group_by_two_level_threshold = 1;
SET max_threads = 4;
SET max_bytes_before_external_group_by = 0;
SET optimize_aggregation_in_order = 0;

INSERT INTO t_uniq_exact(a, b, c) SELECT 0, random_printable_ascii(5), rand() FROM numbers(300000);
INSERT INTO t_uniq_exact(a, b, c) SELECT 1, random_printable_ascii(5), rand() FROM numbers(300000);
INSERT INTO t_uniq_exact(a, b, c) SELECT 2, random_printable_ascii(5), rand() FROM numbers(300000);
INSERT INTO t_uniq_exact(a, b, c) SELECT 3, random_printable_ascii(5), rand() FROM numbers(300000);
INSERT INTO t_uniq_exact(a, b, c) SELECT 4, random_printable_ascii(5), rand() FROM numbers(300000);
INSERT INTO t_uniq_exact(a, b, c) SELECT 5, random_printable_ascii(5), rand() FROM numbers(300000);
INSERT INTO t_uniq_exact(a, b, c) SELECT 6, random_printable_ascii(5), rand() FROM numbers(300000);
INSERT INTO t_uniq_exact(a, b, c) SELECT 7, random_printable_ascii(5), rand() FROM numbers(300000);
INSERT INTO t_uniq_exact(a, b, c) SELECT 8, random_printable_ascii(5), rand() FROM numbers(300000);
INSERT INTO t_uniq_exact(a, b, c) SELECT 9, random_printable_ascii(5), rand() FROM numbers(300000);

SELECT sleep(3) FORMAT Null;
OPTIMIZE STREAM t_uniq_exact FINAL;

SELECT a, uniq_exact(b) FROM table(t_uniq_exact) GROUP BY a ORDER BY a
SETTINGS min_hit_rate_to_use_consecutive_keys_optimization = 1.0
EXCEPT
SELECT a, uniq_exact(b) FROM table(t_uniq_exact) GROUP BY a ORDER BY a
SETTINGS min_hit_rate_to_use_consecutive_keys_optimization = 0.5;

SELECT a, sum(c) FROM table(t_uniq_exact) GROUP BY a ORDER BY a
SETTINGS min_hit_rate_to_use_consecutive_keys_optimization = 1.0
EXCEPT
SELECT a, sum(c) FROM table(t_uniq_exact) GROUP BY a ORDER BY a
SETTINGS min_hit_rate_to_use_consecutive_keys_optimization = 0.5;

DROP STREAM t_uniq_exact;