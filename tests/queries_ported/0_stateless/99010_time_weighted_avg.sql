DROP STREAM IF EXISTS test_99010;

CREATE STREAM test_99010 (val int, a DateTime, b Date, c Date32, d DateTime64);

INSERT INTO test_99010(val, a, b, c, d) VALUES (1, to_datetime('2024-11-29 12:12:13'), '2024-11-29', '2024-11-29', to_datetime64('2024-11-29 12:12:13.123', 3));
INSERT INTO test_99010(val, a, b, c, d) VALUES (2, to_datetime('2024-11-29 12:12:16'), '2024-11-30', '2024-11-30', to_datetime64('2024-11-29 12:12:13.126', 3));
INSERT INTO test_99010(val, a, b, c, d) VALUES (3, to_datetime('2024-11-29 12:12:17'), '2024-12-01', '2024-12-01', to_datetime64('2024-11-29 12:12:13.127', 3));
INSERT INTO test_99010(val, a, b, c, d) VALUES (4, to_datetime('2024-11-29 12:12:18'), '2024-12-03', '2024-12-03', to_datetime64('2024-11-29 12:12:13.128', 3));
INSERT INTO test_99010(val, a, b, c, d) VALUES (5, to_datetime('2024-11-29 12:12:19'), '2024-12-28', '2024-12-28', to_datetime64('2024-11-29 12:12:13.129', 3));
INSERT INTO test_99010(val, a, b, c, d) VALUES (6, to_datetime('2024-11-29 12:12:25'), '2024-12-29', '2024-12-29', to_datetime64('2024-11-29 12:12:13.135', 3));
SELECT sleep(3);

SELECT time_weighted_avg(val, a) FROM (SELECT * FROM table(test_99010) ORDER BY a);
SELECT time_weighted_avg(val, b) FROM (SELECT * FROM table(test_99010) ORDER BY b);
SELECT time_weighted_avg(val, c) FROM (SELECT * FROM table(test_99010) ORDER BY c);
SELECT time_weighted_avg(val, d) FROM (SELECT * FROM table(test_99010) ORDER BY d);
DROP STREAM IF EXISTS test_99010;

