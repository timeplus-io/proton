DROP STREAM IF EXISTS t_nested_modify;

CREATE STREAM t_nested_modify (id uint64, `n.a` array(uint32), `n.b` array(string))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_nested_modify VALUES (1, [2], ['aa']);
INSERT INTO t_nested_modify VALUES (2, [44, 55], ['bb', 'cc']);

SELECT id, `n.a`, `n.b`, to_type_name(`n.b`) FROM t_nested_modify ORDER BY id;

ALTER STREAM t_nested_modify MODIFY COLUMN `n.b` string;

SELECT id, `n.a`, `n.b`, to_type_name(`n.b`) FROM t_nested_modify ORDER BY id;

DETACH STREAM t_nested_modify;
ATTACH STREAM t_nested_modify;

SELECT id, `n.a`, `n.b`, to_type_name(`n.b`) FROM t_nested_modify ORDER BY id;

DROP STREAM t_nested_modify;
