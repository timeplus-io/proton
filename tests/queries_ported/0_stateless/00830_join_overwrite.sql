DROP STREAM IF EXISTS kv;

CREATE STREAM kv (k uint32, v uint32) ENGINE Join(Any, Left, k);
INSERT INTO kv VALUES (1, 2);
INSERT INTO kv VALUES (1, 3);
SELECT join_get('kv', 'v', to_uint32(1));
CREATE STREAM kv_overwrite (k uint32, v uint32) ENGINE Join(Any, Left, k) SETTINGS join_any_take_last_row = 1;
INSERT INTO kv_overwrite VALUES (1, 2);
INSERT INTO kv_overwrite VALUES (1, 3);
SELECT join_get('kv_overwrite', 'v', to_uint32(1));

DROP STREAM kv;
DROP STREAM kv_overwrite;
