DROP STREAM IF EXISTS a;
DROP STREAM IF EXISTS j;

CREATE STREAM a(`id` uint32, `val` uint32) ENGINE = Memory;
CREATE STREAM j(`id` uint32, `val` uint8) ENGINE = Join(ANY, LEFT, id);

INSERT INTO a VALUES (1,1)(2,2)(3,3);
INSERT INTO j VALUES (2,2)(4,4);

SELECT * FROM a ANY LEFT OUTER JOIN j USING id ORDER BY a.id, a.val SETTINGS enable_optimize_predicate_expression = 1;
SELECT * FROM a ANY LEFT OUTER JOIN j USING id ORDER BY a.id, a.val SETTINGS enable_optimize_predicate_expression = 0;

DROP STREAM a;
DROP STREAM j;

CREATE STREAM j (id uint8, val uint8) Engine = Join(ALL, INNER, id);
SELECT * FROM (SELECT 0 as id, 1 as val) as _ JOIN j USING id;

DROP STREAM j;
