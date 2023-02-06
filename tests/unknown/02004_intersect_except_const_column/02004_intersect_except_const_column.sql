-- { echo }
-- Test: crash the server
SELECT 'fooooo' INTERSECT DISTINCT SELECT 'fooooo';
SELECT 'fooooo' EXCEPT ALL SELECT 'fooooo';

-- Test: intersect return incorrect result for const column
SELECT 1 FROM numbers(10) INTERSECT SELECT 1 FROM numbers(10);
SELECT to_string(1) FROM numbers(10) INTERSECT SELECT to_string(1) FROM numbers(10);
SELECT '1' FROM numbers(10) INTERSECT SELECT '1' FROM numbers(10);
SELECT 1 FROM numbers(10) INTERSECT DISTINCT SELECT 1 FROM numbers(10);
SELECT to_string(1) FROM numbers(10) INTERSECT DISTINCT SELECT to_string(1) FROM numbers(10);
SELECT '1' FROM numbers(10) INTERSECT DISTINCT SELECT '1' FROM numbers(10);

-- Test: except return incorrect result for const column
SELECT 2 FROM numbers(10) EXCEPT SELECT 1 FROM numbers(5);
SELECT to_string(2) FROM numbers(10) EXCEPT SELECT to_string(1) FROM numbers(5);
SELECT '2' FROM numbers(10) EXCEPT SELECT '1' FROM numbers(5);
SELECT 2 FROM numbers(10) EXCEPT DISTINCT SELECT 1 FROM numbers(5);
SELECT to_string(2) FROM numbers(10) EXCEPT DISTINCT SELECT to_string(1) FROM numbers(5);
SELECT '2' FROM numbers(10) EXCEPT DISTINCT SELECT '1' FROM numbers(5);