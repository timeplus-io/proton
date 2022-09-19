DROP STREAM IF EXISTS src;
DROP STREAM IF EXISTS dst;
DROP STREAM IF EXISTS original_mv;
DROP STREAM IF EXISTS new_mv;

create stream src (x uint8) ENGINE = Null;
create stream dst (x uint8) ;

CREATE MATERIALIZED VIEW original_mv TO dst AS SELECT * FROM src;

INSERT INTO src VALUES (1), (2);
SELECT * FROM original_mv ORDER BY x;

RENAME TABLE original_mv TO new_mv;

INSERT INTO src VALUES (3);
SELECT * FROM dst ORDER BY x;

SELECT * FROM new_mv ORDER BY x;

DROP STREAM IF EXISTS src;
DROP STREAM IF EXISTS dst;
DROP STREAM IF EXISTS original_mv;
DROP STREAM IF EXISTS new_mv;
