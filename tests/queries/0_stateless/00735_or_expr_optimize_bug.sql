DROP STREAM IF EXISTS or_expr_bug;
create stream or_expr_bug (a uint64, b uint64) ;

INSERT INTO or_expr_bug VALUES(1,21),(1,22),(1,23),(2,21),(2,22),(2,23),(3,21),(3,22),(3,23);

SELECT count(*) FROM or_expr_bug WHERE (a=1 OR a=2 OR a=3) AND (b=21 OR b=22 OR b=23);
DROP STREAM or_expr_bug;
