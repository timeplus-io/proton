DROP STREAM IF EXISTS X;
DROP STREAM IF EXISTS Y;

CREATE STREAM X (id int) ENGINE=Memory;
CREATE STREAM Y (id int) ENGINE=Memory;

SELECT Y.id - 1 FROM X RIGHT JOIN Y ON (X.id + 1) = Y.id SETTINGS join_use_nulls=1;
SELECT Y.id - 1 FROM X RIGHT JOIN Y ON (X.id + 1) = to_int64(Y.id) SETTINGS join_use_nulls=1;

-- Fix issue #20366 
-- Arguments of 'plus' have incorrect data types: '2' of type 'uint8', '1' of type 'uint8'.
-- Because 1 became to_nullable(1), i.e.:
--     2 uint8 Const(size = 1, uint8(size = 1))
--     1 uint8 Const(size = 1, nullable(size = 1, uint8(size = 1), uint8(size = 1)))
SELECT 2+1 FROM system.one X RIGHT JOIN system.one Y ON X.dummy+1 = Y.dummy SETTINGS join_use_nulls = 1;
SELECT 2+1 FROM system.one X RIGHT JOIN system.one Y ON X.dummy+1 = to_uint16(Y.dummy) SETTINGS join_use_nulls = 1;
SELECT X.dummy+1 FROM system.one X RIGHT JOIN system.one Y ON X.dummy = Y.dummy SETTINGS join_use_nulls = 1;
SELECT Y.dummy+1 FROM system.one X RIGHT JOIN system.one Y ON X.dummy = Y.dummy SETTINGS join_use_nulls = 1;

DROP STREAM X;
DROP STREAM Y;
