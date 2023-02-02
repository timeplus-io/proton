-- error cases
SELECT extract_groups();  --{serverError 42} not enough arguments
SELECT extract_groups('hello');  --{serverError 42} not enough arguments
SELECT extract_groups('hello', 123);  --{serverError 43} invalid argument type
SELECT extract_groups(123, 'world');  --{serverError 43}  invalid argument type
SELECT extract_groups('hello world', '((('); --{serverError 427}  invalid re
SELECT extract_groups('hello world', materialize('\\w+')); --{serverError 44} non-const needle

SELECT '0 groups, zero matches';
SELECT extract_groups('hello world', '\\w+'); -- { serverError 36 }

SELECT '1 group, multiple matches, string and fixed_string';
SELECT extract_groups('hello world', '(\\w+) (\\w+)');
SELECT extract_groups('hello world', CAST('(\\w+) (\\w+)' as fixed_string(11)));
SELECT extract_groups(CAST('hello world' AS fixed_string(12)), '(\\w+) (\\w+)');
SELECT extract_groups(CAST('hello world' AS fixed_string(12)), CAST('(\\w+) (\\w+)' as fixed_string(11)));
SELECT extract_groups(materialize(CAST('hello world' AS fixed_string(12))), '(\\w+) (\\w+)');
SELECT extract_groups(materialize(CAST('hello world' AS fixed_string(12))), CAST('(\\w+) (\\w+)' as fixed_string(11)));

SELECT 'multiple matches';
SELECT extract_groups('abc=111, def=222, ghi=333 "jkl mno"="444 foo bar"', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');

SELECT 'big match';
SELECT
    length(haystack), length(matches), array_map((x) -> length(x), matches)
FROM (
    SELECT
        repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
        extract_groups(haystack, '(abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz)') AS matches
    FROM numbers(3)
);

SELECT 'lots of matches';
SELECT
    length(haystack), length(matches), array_reduce('sum', array_map((x) -> length(x), matches))
FROM (
    SELECT
        repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
        extract_groups(haystack, '(\\w)') AS matches
    FROM numbers(3)
);

SELECT 'lots of groups';
SELECT
    length(haystack), length(matches), array_map((x) -> length(x), matches)
FROM (
    SELECT
        repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
        extract_groups(haystack, repeat('(\\w)', 100)) AS matches
    FROM numbers(3)
);
