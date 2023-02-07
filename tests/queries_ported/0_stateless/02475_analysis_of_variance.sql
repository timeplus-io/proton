
SELECT analysis_of_variance(number, number % 2) FROM numbers(10) FORMAT Null;
SELECT analysis_of_variance(number :: decimal32(5), number % 2) FROM numbers(10) FORMAT Null;
SELECT analysis_of_variance(number :: decimal256(5), number % 2) FROM numbers(10) FORMAT Null;

SELECT analysis_of_variance(1.11, -20); -- { serverError BAD_ARGUMENTS }
SELECT analysis_of_variance(1.11, 20 :: uint128); -- { serverError BAD_ARGUMENTS }
SELECT analysis_of_variance(1.11, 9000000000000000); -- { serverError BAD_ARGUMENTS }

SELECT analysis_of_variance(number, number % 2), analysis_of_variance(100000000000000000000., number % 65535) FROM numbers(1048575); -- { serverError BAD_ARGUMENTS }
