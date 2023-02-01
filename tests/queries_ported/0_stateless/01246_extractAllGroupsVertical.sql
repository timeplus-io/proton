-- error cases
SELECT extract_all_groups_vertical();  --{serverError 42} not enough arguments
SELECT extract_all_groups_vertical('hello');  --{serverError 42} not enough arguments
SELECT extract_all_groups_vertical('hello', 123);  --{serverError 43} invalid argument type
SELECT extract_all_groups_vertical(123, 'world');  --{serverError 43}  invalid argument type
SELECT extract_all_groups_vertical('hello world', '((('); --{serverError 427}  invalid re
SELECT extract_all_groups_vertical('hello world', materialize('\\w+')); --{serverError 44} non-const needle
SELECT extract_all_groups_vertical('hello world', '\\w+'); -- { serverError 36 } 0 groups

SELECT '1 group, multiple matches, string and fixed_string';
SELECT extract_all_groups_vertical('hello world', '(\\w+)');
SELECT extract_all_groups_vertical('hello world', CAST('(\\w+)' as fixed_string(5)));
SELECT extract_all_groups_vertical(CAST('hello world' AS fixed_string(12)), '(\\w+)');
SELECT extract_all_groups_vertical(CAST('hello world' AS fixed_string(12)), CAST('(\\w+)' as fixed_string(5)));
SELECT extract_all_groups_vertical(materialize(CAST('hello world' AS fixed_string(12))), '(\\w+)');
SELECT extract_all_groups_vertical(materialize(CAST('hello world' AS fixed_string(12))), CAST('(\\w+)' as fixed_string(5)));

SELECT 'mutiple groups, multiple matches';
SELECT extract_all_groups_vertical('abc=111, def=222, ghi=333 "jkl mno"="444 foo bar"', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');

SELECT 'big match';
SELECT
    length(haystack), length(matches[1]), length(matches), array_map((x) -> length(x), array_map(x -> x[1], matches))
FROM (
    SELECT
        repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
        extract_all_groups_vertical(haystack, '(abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz)') AS matches
    FROM numbers(3)
);

SELECT 'lots of matches';
SELECT
    length(haystack), length(matches[1]), length(matches), array_reduce('sum', array_map((x) -> length(x), array_map(x -> x[1], matches)))
FROM (
    SELECT
        repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
        extract_all_groups_vertical(haystack, '(\\w)') AS matches
    FROM numbers(3)
);

SELECT 'lots of groups';
SELECT
    length(haystack), length(matches[1]), length(matches), array_map((x) -> length(x), array_map(x -> x[1], matches))
FROM (
    SELECT
        repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
        extract_all_groups_vertical(haystack, repeat('(\\w)', 100)) AS matches
    FROM numbers(3)
);
