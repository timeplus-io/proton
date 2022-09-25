-- Tags: no-parallel

DROP STREAM IF EXISTS alter_attach;
create stream alter_attach (x uint64, p uint8) ENGINE = MergeTree ORDER BY tuple() PARTITION BY p;
INSERT INTO alter_attach VALUES (1, 1), (2, 1), (3, 1);

ALTER STREAM alter_attach DETACH PARTITION 1;

ALTER STREAM alter_attach ADD COLUMN s string;
INSERT INTO alter_attach VALUES (4, 2, 'Hello'), (5, 2, 'World');

ALTER STREAM alter_attach ATTACH PARTITION 1;
SELECT * FROM alter_attach ORDER BY x;

ALTER STREAM alter_attach DETACH PARTITION 2;
ALTER STREAM alter_attach DROP COLUMN s;
INSERT INTO alter_attach VALUES (6, 3), (7, 3);

ALTER STREAM alter_attach ATTACH PARTITION 2;
SELECT * FROM alter_attach ORDER BY x;

DROP STREAM alter_attach;
