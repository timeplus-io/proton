SET query_mode = 'table';
CREATE STREAM t (st fixed_string(54));

INSERT INTO t (st) VALUES 
('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRTUVWXYZ'), ('\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0'), ('IIIIIIIIII\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0');

SELECT sleep(3);
WITH (SELECT group_concat(st, ',') FROM t) AS a,
     (SELECT group_concat(st :: string, ',') FROM t) AS b
SELECT equals(a, b);
