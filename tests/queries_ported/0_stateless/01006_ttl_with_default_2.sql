DROP STREAM IF EXISTS ttl_with_default;

CREATE STREAM ttl_with_default (d DateTime, a int default 777 ttl d + interval 5 SECOND) ENGINE = MergeTree ORDER BY d;
INSERT INTO ttl_with_default VALUES (now() - 1000, 1) (now() - 1000, 2) (now() + 1000, 3)(now() + 1000, 4);
SELECT sleep(0.7) FORMAT Null; -- wait if very fast merge happen
OPTIMIZE TABLE ttl_with_default FINAL;

-- check that after second merge there are still user defaults in column
SELECT sleep(0.7) FORMAT Null;
OPTIMIZE TABLE ttl_with_default FINAL;

SELECT a FROM ttl_with_default ORDER BY a;

DROP STREAM ttl_with_default;
