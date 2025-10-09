SELECT (number * 2)::int128 FROM numbers(10) ORDER BY 1 ASC WITH FILL FROM 3 TO 8;
SELECT (number * 2)::int256 FROM numbers(10) ORDER BY 1 ASC WITH FILL FROM 3 TO 8;
SELECT (number * 2)::uint128 FROM numbers(10) ORDER BY 1 ASC WITH FILL FROM 3 TO 8;
SELECT (number * 2)::uint256 FROM numbers(10) ORDER BY 1 ASC WITH FILL FROM 3 TO 8;

SELECT (number * 2)::int128 as result FROM numbers(10) ORDER BY result ASC WITH FILL FROM -3 TO 5;
SELECT (number * 2)::int256 as result FROM numbers(10) ORDER BY result ASC WITH FILL FROM -3 TO 5;
SELECT (number * 2)::uint128 as result FROM numbers(10) ORDER BY result ASC WITH FILL FROM -3 TO 5; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT (number * 2)::uint256 as result FROM numbers(10) ORDER BY result ASC WITH FILL FROM -3 TO 5; -- { serverError ARGUMENT_OUT_OF_BOUND }