-- error cases
SELECT extract_all_groups_horizontal();  --{serverError 42} not enough arguments
SELECT extract_all_groups_horizontal('hello');  --{serverError 42} not enough arguments
SELECT extract_all_groups_horizontal('hello', 123);  --{serverError 43} invalid argument type
SELECT extract_all_groups_horizontal(123, 'world');  --{serverError 43}  invalid argument type
SELECT extract_all_groups_horizontal('hello world', '((('); --{serverError 427}  invalid re
SELECT extract_all_groups_horizontal('hello world', materialize('\\w+')); --{serverError 44} non-cons needle
SELECT extract_all_groups_horizontal('hello world', '\\w+');  -- { serverError 36 } 0 groups
SELECT extract_all_groups_horizontal('hello world', '(\\w+)') SETTINGS regexp_max_matches_per_row = 0;  -- { serverError 128 } to many groups matched per row
SELECT extract_all_groups_horizontal('hello world', '(\\w+)') SETTINGS regexp_max_matches_per_row = 1;  -- { serverError 128 } to many groups matched per row

SELECT extract_all_groups_horizontal('hello world', '(\\w+)') SETTINGS regexp_max_matches_per_row = 1000000 FORMAT Null; -- users now can set limit bigger than previous 1000 matches per row

SELECT '1 group, multiple matches, string and fixed_string';
SELECT extract_all_groups_horizontal('hello world', '(\\w+)');
SELECT extract_all_groups_horizontal('hello world', CAST('(\\w+)' as fixed_string(5)));
SELECT extract_all_groups_horizontal(CAST('hello world' AS fixed_string(12)), '(\\w+)');
SELECT extract_all_groups_horizontal(CAST('hello world' AS fixed_string(12)), CAST('(\\w+)' as fixed_string(5)));
SELECT extract_all_groups_horizontal(materialize(CAST('hello world' AS fixed_string(12))), '(\\w+)');
SELECT extract_all_groups_horizontal(materialize(CAST('hello world' AS fixed_string(12))), CAST('(\\w+)' as fixed_string(5)));

SELECT 'mutiple groups, multiple matches';
SELECT extract_all_groups_horizontal('abc=111, def=222, ghi=333 "jkl mno"="444 foo bar"', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');

SELECT 'big match';
SELECT
    length(haystack), length(matches), length(matches[1]), array_map((x) -> length(x), matches[1])
FROM (
    SELECT
           repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
           extract_all_groups_horizontal(haystack, '(abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz)') AS matches
    FROM numbers(3)
);

SELECT 'lots of matches';
SELECT
    length(haystack), length(matches), length(matches[1]), array_reduce('sum', array_map((x) -> length(x), matches[1]))
FROM (
    SELECT
        repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
        extract_all_groups_horizontal(haystack, '(\\w)') AS matches
    FROM numbers(3)
);

SELECT 'lots of groups';
SELECT
    length(haystack), length(matches), length(matches[1]), array_map((x) -> length(x), matches[1])
FROM (
    SELECT
        repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
        extract_all_groups_horizontal(haystack, repeat('(\\w)', 100)) AS matches
    FROM numbers(3)
);
