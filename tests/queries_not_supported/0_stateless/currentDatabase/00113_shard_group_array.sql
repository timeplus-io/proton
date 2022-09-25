-- Tags: shard

SELECT int_div(number, 100) AS k, length(group_array(number)) FROM (SELECT * FROM system.numbers LIMIT 1000000) GROUP BY k WITH TOTALS ORDER BY k LIMIT 10;

SELECT '';
SELECT length(to_string(groupArrayState(to_date(number)))) FROM (SELECT * FROM system.numbers LIMIT 10);
SELECT length(to_string(groupArrayState(to_datetime(number)))) FROM (SELECT * FROM system.numbers LIMIT 10);

DROP STREAM IF EXISTS numbers_mt;
create stream numbers_mt (number uint64)  ;
INSERT INTO numbers_mt SELECT * FROM system.numbers LIMIT 1, 1000000;

SELECT count(), sum(ns), max(ns) FROM (SELECT int_div(number, 100) AS k, group_array(number) AS ns FROM numbers_mt GROUP BY k) ARRAY JOIN ns;
SELECT count(), sum(to_uint64(ns)), max(to_uint64(ns)) FROM (SELECT int_div(number, 100) AS k, group_array(to_string(number)) AS ns FROM numbers_mt GROUP BY k) ARRAY JOIN ns;
SELECT count(), sum(to_uint64(ns[1])), max(to_uint64(ns[1])), sum(to_uint64(ns[2]))/10 FROM (SELECT int_div(number, 100) AS k, group_array([to_string(number), to_string(number*10)]) AS ns FROM numbers_mt GROUP BY k) ARRAY JOIN ns;
SELECT count(), sum(ns[1]), max(ns[1]), sum(ns[2])/10 FROM (SELECT int_div(number, 100) AS k, group_array([number, number*10]) AS ns FROM numbers_mt GROUP BY k) ARRAY JOIN ns;

SELECT count(), sum(ns), max(ns) FROM (SELECT int_div(number, 100) AS k, group_array(number) AS ns FROM remote('127.0.0.{2,3}', currentDatabase(), 'numbers_mt') GROUP BY k) ARRAY JOIN ns;
SELECT count(), sum(to_uint64(ns)), max(to_uint64(ns)) FROM (SELECT int_div(number, 100) AS k, group_array(to_string(number)) AS ns FROM remote('127.0.0.{2,3}', currentDatabase(), 'numbers_mt') GROUP BY k) ARRAY JOIN ns;
SELECT count(), sum(to_uint64(ns[1])), max(to_uint64(ns[1])), sum(to_uint64(ns[2]))/10 FROM (SELECT int_div(number, 100) AS k, group_array([to_string(number), to_string(number*10)]) AS ns FROM remote('127.0.0.{2,3}', currentDatabase(), 'numbers_mt') GROUP BY k) ARRAY JOIN ns;

DROP STREAM numbers_mt;
create stream numbers_mt (number uint64)  ;
INSERT INTO numbers_mt SELECT * FROM system.numbers LIMIT 1, 1048575;

SELECT '';
SELECT roundToExp2(number) AS k, length(group_array(1)(number AS i)), length(group_array(1024)(i)), length(group_array(65536)(i)) AS s FROM numbers_mt GROUP BY k ORDER BY k LIMIT 9, 11;
SELECT roundToExp2(number) AS k, length(group_array(1)(hex(number) AS i)), length(group_array(1024)(i)), length(group_array(65536)(i)) AS s FROM numbers_mt GROUP BY k ORDER BY k LIMIT 9, 11;
SELECT roundToExp2(number) AS k, length(group_array(1)([hex(number)] AS i)), length(group_array(1024)(i)), length(group_array(65536)(i)) AS s FROM numbers_mt GROUP BY k ORDER BY k LIMIT 9, 11;

SELECT '';
SELECT roundToExp2(number) AS k, length(group_array(1)(number AS i)), length(group_array(1500)(i)), length(group_array(70000)(i)) AS s FROM remote('127.0.0.{2,3}', currentDatabase(), 'numbers_mt') GROUP BY k ORDER BY k LIMIT 9, 11;
SELECT roundToExp2(number) AS k, length(group_array(1)(hex(number) AS i)), length(group_array(1500)(i)), length(group_array(70000)(i)) AS s FROM remote('127.0.0.{2,3}', currentDatabase(), 'numbers_mt') GROUP BY k ORDER BY k LIMIT 9, 11;
SELECT roundToExp2(number) AS k, length(group_array(1)([hex(number)] AS i)), length(group_array(1500)(i)), length(group_array(70000)(i)) AS s FROM remote('127.0.0.{2,3}', currentDatabase(), 'numbers_mt') GROUP BY k ORDER BY k LIMIT 9, 11;

DROP STREAM numbers_mt;

-- Check binary compatibility:
-- clickhouse-client -h old -q "SELECT array_reduce('groupArrayState', [['1'], ['22'], ['333']]) FORMAT RowBinary" | clickhouse-local -s --input-format RowBinary --structure "d aggregate_function(groupArray2, array(string))" -q "SELECT groupArray2Merge(d) FROM table"
