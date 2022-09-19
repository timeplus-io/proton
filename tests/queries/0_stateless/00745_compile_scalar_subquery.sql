SET compile_expressions = 1;
SET min_count_to_compile_expression = 1;
SET optimize_move_to_prewhere = 0;

DROP STREAM IF EXISTS dt;
DROP STREAM IF EXISTS testx;

create stream dt(tkey int32) ENGINE = MergeTree order by tuple();
INSERT INTO dt VALUES (300000);
create stream testx(t int32, a uint8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO testx VALUES (100000, 0);

SELECT COUNT(*) FROM testx WHERE NOT a AND t < (SELECT tkey FROM dt);

DROP STREAM dt;
create stream dt(tkey int32) ENGINE = MergeTree order by tuple();
INSERT INTO dt VALUES (0);

SELECT COUNT(*) FROM testx WHERE NOT a AND t < (SELECT tkey FROM dt);

DROP STREAM IF EXISTS dt;
DROP STREAM IF EXISTS testx;
