SELECT sum(1) FROM (SELECT * FROM system.numbers LIMIT 1000);
SELECT sum_with_overflow(1) FROM (SELECT * FROM system.numbers LIMIT 1000);
SELECT sum_kahan(1e100) - 1e100 * 1000 FROM (SELECT * FROM system.numbers LIMIT 1000);
SELECT abs(sum(1e100) - 1e100 * 1000) > 1 FROM (SELECT * FROM system.numbers LIMIT 1000);
