DROP STREAM if exists test_float_precise;

-- Test precise_float_parsing with VALUES parsing
CREATE STREAM test_float_precise (id uint32, value float64);
select sleep(2) FORMAT Null;

-- Test without precise_float_parsing (should use fast parsing)
INSERT INTO test_float_precise(id, value) settings precise_float_parsing=0 VALUES (1, 1.234567891209E12);
select sleep(1) FORMAT Null;

-- Test with precise_float_parsing=1 (should use precise parsing)
INSERT INTO test_float_precise(id, value) SETTINGS precise_float_parsing=1 VALUES (2, 1.234567891209E12);
select sleep(1) FORMAT Null;

-- Test without precise_float_parsing (should use precise parsing)
INSERT INTO test_float_precise(id, value)  VALUES (3, 1.234567891209E12);
select sleep(2) FORMAT Null;

select sleep(2) FORMAT Null;

-- Verify the results
SELECT id, value FROM table(test_float_precise) ORDER BY id;

DROP STREAM test_float_precise;
