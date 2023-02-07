SELECT max_map([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT max_map([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT max_map([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;

SELECT minMap([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT minMap([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT minMap([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;

SELECT sumMap([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT sumMap([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT sumMap([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;

SELECT '-';

SELECT max_map([number % 3, number % 4 - 1], [number :: float64, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT max_map([number % 3, number % 4 - 1], [number :: float64, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT max_map([number % 3, number % 4 - 1], [number :: float64, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;

SELECT minMap([number % 3, number % 4 - 1], [number :: float64, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT minMap([number % 3, number % 4 - 1], [number :: float64, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT minMap([number % 3, number % 4 - 1], [number :: float64, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;

SELECT sumMap([number % 3, number % 4 - 1], [number :: float64, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT sumMap([number % 3, number % 4 - 1], [number :: float64, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT sumMap([number % 3, number % 4 - 1], [number :: float64, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;

SELECT '-';

SELECT max_map([number % 3, number % 4 - 1], [number :: uint256, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT max_map([number % 3, number % 4 - 1], [number :: uint256, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT max_map([number % 3, number % 4 - 1], [number :: uint256, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;

SELECT minMap([number % 3, number % 4 - 1], [number :: uint256, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT minMap([number % 3, number % 4 - 1], [number :: uint256, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT minMap([number % 3, number % 4 - 1], [number :: uint256, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;

SELECT sumMap([number % 3, number % 4 - 1], [number :: uint256, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT sumMap([number % 3, number % 4 - 1], [number :: uint256, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT sumMap([number % 3, number % 4 - 1], [number :: uint256, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;
