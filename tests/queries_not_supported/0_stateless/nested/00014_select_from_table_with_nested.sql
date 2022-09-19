-- Tags: bug, #1305
SET query_mode = 'table';
DROP STREAM IF EXISTS nested_test;
create stream nested_test (s string, nest nested(x uint8, y uint32));
INSERT INTO nested_test(s, nest) VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], []);
SELECT sleep(3);
SELECT * FROM nested_test;
SELECT s, nest.x, nest.y FROM nested_test ARRAY JOIN nest;
SELECT s, nest.x, nest.y FROM nested_test ARRAY JOIN nest.x;
SELECT s, nest.x, nest.y FROM nested_test ARRAY JOIN nest.x, nest.y;
SELECT s, n.x, n.y FROM nested_test ARRAY JOIN nest AS n;
SELECT s, n.x, n.y, nest.x FROM nested_test ARRAY JOIN nest AS n;
SELECT s, n.x, n.y, nest.x, nest.y FROM nested_test ARRAY JOIN nest AS n;
SELECT s, n.x, n.y, nest.x, nest.y, num FROM nested_test ARRAY JOIN nest AS n, array_enumerate(nest.x) AS num;
DROP STREAM nested_test;
