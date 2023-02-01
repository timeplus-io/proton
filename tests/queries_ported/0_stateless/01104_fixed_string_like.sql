SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE 'hello';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE 'world';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE 'xyz';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE 'hell';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE 'orld';

SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%hello%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%world%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%xyz%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%hell%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%orld%';

SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%hello';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%world';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%xyz';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%hell';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%orld';

SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE 'hello%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE 'world%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE 'xyz%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE 'hell%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE 'orld%';

SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%he%o%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%w%ld%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%x%z%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%hell_';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '_orld%';

SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%he__o%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%w__ld%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%x%z%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE 'hell_';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '_orld';

SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE 'helloworld';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%helloworld%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%elloworl%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%ow%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%o%w%';

SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%o%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%l%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%l%o%';
SELECT array_join(CAST(['hello', 'world'] AS array(fixed_string(5)))) LIKE '%o%l%';
