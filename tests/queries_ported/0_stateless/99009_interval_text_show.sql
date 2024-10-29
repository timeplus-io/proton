DROP VIEW IF EXISTS v_99009;
DROP STREAM IF EXISTS test_99009;

CREATE STREAM test_99009 (id int, value int);

INSERT INTO test_99009(id, value) VALUES (1, 2);
INSERT INTO test_99009(id, value) VALUES (1, 8);

CREATE VIEW v_99009 AS SELECT count() FROM test_99009 WHERE _tp_time > now() - interval '1 minute' + interval '-1 hour' + interval '+1 second';

SHOW CREATE v_99009;

SELECT * FROM v_99009 LIMIT 1;

DROP VIEW IF EXISTS v_99009;
DROP STREAM IF EXISTS test_99009;
