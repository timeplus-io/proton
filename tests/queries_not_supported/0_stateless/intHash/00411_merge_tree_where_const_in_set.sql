DROP STREAM IF EXISTS const_in_const;
create stream const_in_const (id uint64, date date, uid uint32, name string, Sign int8) ENGINE = CollapsingMergeTree(date, intHash32(uid), (id, date, intHash32(uid)), 8192, Sign);
INSERT INTO const_in_const VALUES(1, now(), 1, 'test1', 1);
INSERT INTO const_in_const VALUES(2, now(), 1, 'test2', 1);
INSERT INTO const_in_const VALUES(3, now(), 1, 'test3', 1);
INSERT INTO const_in_const VALUES(4, now(), 2, 'test4', 1);
INSERT INTO const_in_const VALUES(5, now(), 3, 'test5', 1);

SELECT 1 from const_in_const where 42 in (225);
SELECT name FROM const_in_const WHERE 1 IN (125, 1, 2) ORDER BY name LIMIT 1;

DROP STREAM IF EXISTS const_in_const;
