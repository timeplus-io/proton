DROP STREAM IF EXISTS t_having;

create stream t_having (c0 int32, c1 uint64) ;

INSERT INTO t_having SELECT number, number FROM numbers(1000);

SELECT sum(c0 = 0), min(c0 + 1), sum(c0 + 2) FROM t_having
GROUP BY c0 HAVING c0 = 0
SETTINGS enable_optimize_predicate_expression=0;

SELECT c0 + -1, sum(int_div_or_zero(int_div_or_zero(NULL, NULL), '2'), int_div_or_zero(10000000000., int_div_or_zero(int_div_or_zero(int_div_or_zero(NULL, NULL), 10), NULL))) FROM t_having GROUP BY c0 = 2, c0 = 10, int_div_or_zero(int_div_or_zero(int_div_or_zero(NULL, NULL), NULL), NULL), c0 HAVING c0 = 2 SETTINGS enable_optimize_predicate_expression = 0;

SELECT sum(c0 + 257) FROM t_having GROUP BY c0 = -9223372036854775808, NULL, -2147483649, c0 HAVING c0 = -9223372036854775808 SETTINGS enable_optimize_predicate_expression = 0;

SELECT c0 + -2, c0 + -9223372036854775807, c0 = NULL FROM t_having GROUP BY c0 = 0.9998999834060669, 1023, c0 HAVING c0 = 0.9998999834060669 SETTINGS enable_optimize_predicate_expression = 0;

DROP STREAM t_having;
