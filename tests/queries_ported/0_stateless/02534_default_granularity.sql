DROP STREAM IF EXISTS users_02534;
CREATE STREAM users_02534 (id int16, name string, INDEX bf_idx(name) TYPE minmax);
SHOW CREATE STREAM users_02534;
DROP STREAM users_02534;

CREATE STREAM users_02534 (id int16, name string);
ALTER STREAM users_02534 ADD INDEX bf_idx(name) TYPE minmax;
SHOW CREATE STREAM users_02534;
DROP STREAM users_02534;
