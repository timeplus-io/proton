DROP STREAM IF EXISTS 99010_test_stream;
CREATE STREAM 99010_test_stream(id int);

SELECT sleep(2) FORMAT Null;

INSERT INTO 99010_test_stream(id, _tp_time) VALUES (1, '2024-01-01 00:00:00.1');
INSERT INTO 99010_test_stream(id, _tp_time) VALUES (2, '2024-01-01 00:00:00.2');
INSERT INTO 99010_test_stream(id, _tp_time) VALUES (3, '2024-01-01 00:00:00.3');
INSERT INTO 99010_test_stream(id, _tp_time) VALUES (1, '2024-01-01 00:00:00.4');

SELECT sleep(3) FORMAT Null;

with cte as (select _tp_time, * from table(99010_test_stream)) select count() from dedup(cte, id);
with cte as (select _tp_time, * from table(99010_test_stream) order by _tp_time asc) select * from dedup(cte, id);

DROP STREAM 99010_test_stream;
