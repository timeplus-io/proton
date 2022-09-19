DROP STREAM IF EXISTS tab;

create stream tab (x uint32, y uint32) ENGINE = MergeTree() ORDER BY x;

INSERT INTO tab VALUES (1,1),(1,2),(1,3),(1,4),(1,5);

INSERT INTO tab VALUES (2,6),(2,7),(2,8),(2,9),(2,0);

SELECT * FROM tab ORDER BY x LIMIT 3;
SELECT * FROM tab ORDER BY x LIMIT 4;

DROP STREAM IF EXISTS tab;
