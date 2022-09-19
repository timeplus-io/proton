DROP STREAM IF EXISTS nested1;
DROP STREAM IF EXISTS nested2;

create stream nested1 (d date DEFAULT '2000-01-01', x uint64, n nested(a string, b string)) ENGINE = MergeTree(d, x, 1);
INSERT INTO nested1 (x, n.a, n.b) VALUES (1, ['Hello', 'World'], ['abc', 'def']), (2, [], []);

SET max_block_size = 1;
SELECT * FROM nested1 ORDER BY x;

create stream nested2 (d date DEFAULT '2000-01-01', x uint64, n nested(a string, b string)) ENGINE = MergeTree(d, x, 1);

INSERT INTO nested2 SELECT * FROM nested1;

SELECT * FROM nested2 ORDER BY x;

DROP STREAM nested1;
DROP STREAM nested2;
