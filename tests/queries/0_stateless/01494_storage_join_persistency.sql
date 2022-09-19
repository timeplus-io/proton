-- Tags: no-parallel

DROP STREAM IF EXISTS join;

SELECT '----- Default Settings -----';
create stream join (k uint64, s string) ENGINE = Join(ANY, LEFT, k);
INSERT INTO join VALUES (1,21);
DETACH TABLE join;
ATTACH TABLE join;
SELECT * from join;

DROP STREAM join;

SELECT '----- Settings persistent=1 -----';
create stream join (k uint64, s string) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent=1;
INSERT INTO join VALUES (1,21);
DETACH TABLE join;
ATTACH TABLE join;
SELECT * from join;

DROP STREAM join;

SELECT '----- Settings persistent=0 -----';
create stream join (k uint64, s string) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent=0;
INSERT INTO join VALUES (1,21);
DETACH TABLE join;
ATTACH TABLE join;
SELECT * from join;

DROP STREAM join;
