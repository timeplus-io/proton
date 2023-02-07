-- { echoOn }
SELECT arg_max((n, n), n) as t, to_type_name(t) FROM (SELECT if(number >= 100, NULL, number) AS n from numbers(10));
SELECT arg_max((n, n), n) as t, to_type_name(t) FROM (SELECT if(number <= 100, NULL, number) AS n from numbers(10));
SELECT arg_max((n, n), n) as t, to_type_name(t) FROM (SELECT if(number % 3 = 0, NULL, number) AS n from numbers(10));

SELECT arg_max((n, n), n) as t, to_type_name(t) FROM (SELECT if(number >= 100, NULL, number::int32) AS n from numbers(10));
SELECT arg_max((n, n), n) as t, to_type_name(t) FROM (SELECT if(number <= 100, NULL, number::int32) AS n from numbers(10));
SELECT arg_max((n, n), n) as t, to_type_name(t) FROM (SELECT if(number % 3 = 0, NULL, number::int32) AS n from numbers(10));

SELECT argMin((n, n), n) as t, to_type_name(t) FROM (SELECT if(number >= 100, NULL, number) AS n from numbers(5, 10));
SELECT argMin((n, n), n) as t, to_type_name(t) FROM (SELECT if(number <= 100, NULL, number) AS n from numbers(5, 10));
SELECT argMin((n, n), n) as t, to_type_name(t) FROM (SELECT if(number % 5 == 0, NULL, number) as n from numbers(5, 10));

SELECT argMin((n, n), n) as t, to_type_name(t) FROM (SELECT if(number >= 100, NULL, number::int32) AS n from numbers(5, 10));
SELECT argMin((n, n), n) as t, to_type_name(t) FROM (SELECT if(number <= 100, NULL, number::int32) AS n from numbers(5, 10));
SELECT argMin((n, n), n) as t, to_type_name(t) FROM (SELECT if(number % 5 == 0, NULL, number::int32) as n from numbers(5, 10));

SELECT arg_maxIf((n, n), n, n > 100) as t, to_type_name(t) FROM (SELECT if(number % 3 = 0, NULL, number) AS n from numbers(50));
SELECT arg_maxIf((n, n), n, n < 100) as t, to_type_name(t) FROM (SELECT if(number % 3 = 0, NULL, number) AS n from numbers(50));
SELECT arg_maxIf((n, n), n, n % 5 == 0) as t, to_type_name(t) FROM (SELECT if(number % 3 = 0, NULL, number) AS n from numbers(50));
