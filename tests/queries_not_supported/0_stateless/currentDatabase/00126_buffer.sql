SET query_mode = 'table';
drop stream IF EXISTS buffer_00126;
drop stream IF EXISTS null_sink_00126;

create stream null_sink_00126 (a uint8, b string, c array(uint32)) ENGINE = Null;
create stream buffer_00126 (a uint8, b string, c array(uint32)) ENGINE = Buffer(currentDatabase(), null_sink_00126, 1, 1000, 1000, 1000, 1000, 1000000, 1000000);

INSERT INTO buffer_00126 VALUES (1, '2', [3]);

SELECT a, b, c FROM buffer_00126 ORDER BY a, b, c;
SELECT b, c, a FROM buffer_00126 ORDER BY a, b, c;
SELECT c, a, b FROM buffer_00126 ORDER BY a, b, c;
SELECT a, c, b FROM buffer_00126 ORDER BY a, b, c;
SELECT b, a, c FROM buffer_00126 ORDER BY a, b, c;
SELECT c, b, a FROM buffer_00126 ORDER BY a, b, c;
SELECT a, b FROM buffer_00126 ORDER BY a, b, c;
SELECT b, c FROM buffer_00126 ORDER BY a, b, c;
SELECT c, a FROM buffer_00126 ORDER BY a, b, c;
SELECT a, c FROM buffer_00126 ORDER BY a, b, c;
SELECT b, a FROM buffer_00126 ORDER BY a, b, c;
SELECT c, b FROM buffer_00126 ORDER BY a, b, c;
SELECT a FROM buffer_00126 ORDER BY a, b, c;
SELECT b FROM buffer_00126 ORDER BY a, b, c;
SELECT c FROM buffer_00126 ORDER BY a, b, c;

INSERT INTO buffer_00126 (c, b, a) VALUES ([7], '8', 9);

SELECT a, b, c FROM buffer_00126 ORDER BY a, b, c;
SELECT b, c, a FROM buffer_00126 ORDER BY a, b, c;
SELECT c, a, b FROM buffer_00126 ORDER BY a, b, c;
SELECT a, c, b FROM buffer_00126 ORDER BY a, b, c;
SELECT b, a, c FROM buffer_00126 ORDER BY a, b, c;
SELECT c, b, a FROM buffer_00126 ORDER BY a, b, c;
SELECT a, b FROM buffer_00126 ORDER BY a, b, c;
SELECT b, c FROM buffer_00126 ORDER BY a, b, c;
SELECT c, a FROM buffer_00126 ORDER BY a, b, c;
SELECT a, c FROM buffer_00126 ORDER BY a, b, c;
SELECT b, a FROM buffer_00126 ORDER BY a, b, c;
SELECT c, b FROM buffer_00126 ORDER BY a, b, c;
SELECT a FROM buffer_00126 ORDER BY a, b, c;
SELECT b FROM buffer_00126 ORDER BY a, b, c;
SELECT c FROM buffer_00126 ORDER BY a, b, c;

INSERT INTO buffer_00126 (a, c) VALUES (11, [33]);

SELECT a, b, c FROM buffer_00126 ORDER BY a, b, c;
SELECT b, c, a FROM buffer_00126 ORDER BY a, b, c;
SELECT c, a, b FROM buffer_00126 ORDER BY a, b, c;
SELECT a, c, b FROM buffer_00126 ORDER BY a, b, c;
SELECT b, a, c FROM buffer_00126 ORDER BY a, b, c;
SELECT c, b, a FROM buffer_00126 ORDER BY a, b, c;
SELECT a, b FROM buffer_00126 ORDER BY a, b, c;
SELECT b, c FROM buffer_00126 ORDER BY a, b, c;
SELECT c, a FROM buffer_00126 ORDER BY a, b, c;
SELECT a, c FROM buffer_00126 ORDER BY a, b, c;
SELECT b, a FROM buffer_00126 ORDER BY a, b, c;
SELECT c, b FROM buffer_00126 ORDER BY a, b, c;
SELECT a FROM buffer_00126 ORDER BY a, b, c;
SELECT b FROM buffer_00126 ORDER BY a, b, c;
SELECT c FROM buffer_00126 ORDER BY a, b, c;

drop stream buffer_00126;
drop stream null_sink_00126;
