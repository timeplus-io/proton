SELECT array_join(['hello', 'world']) LIKE 'hello';
SELECT array_join(['hello', 'world']) LIKE 'world';
SELECT array_join(['hello', 'world']) LIKE 'xyz';
SELECT array_join(['hello', 'world']) LIKE 'hell';
SELECT array_join(['hello', 'world']) LIKE 'orld';

SELECT array_join(['hello', 'world']) LIKE '%hello%';
SELECT array_join(['hello', 'world']) LIKE '%world%';
SELECT array_join(['hello', 'world']) LIKE '%xyz%';
SELECT array_join(['hello', 'world']) LIKE '%hell%';
SELECT array_join(['hello', 'world']) LIKE '%orld%';

SELECT array_join(['hello', 'world']) LIKE '%hello';
SELECT array_join(['hello', 'world']) LIKE '%world';
SELECT array_join(['hello', 'world']) LIKE '%xyz';
SELECT array_join(['hello', 'world']) LIKE '%hell';
SELECT array_join(['hello', 'world']) LIKE '%orld';

SELECT array_join(['hello', 'world']) LIKE 'hello%';
SELECT array_join(['hello', 'world']) LIKE 'world%';
SELECT array_join(['hello', 'world']) LIKE 'xyz%';
SELECT array_join(['hello', 'world']) LIKE 'hell%';
SELECT array_join(['hello', 'world']) LIKE 'orld%';

SELECT array_join(['hello', 'world']) LIKE '%he%o%';
SELECT array_join(['hello', 'world']) LIKE '%w%ld%';
SELECT array_join(['hello', 'world']) LIKE '%x%z%';
SELECT array_join(['hello', 'world']) LIKE '%hell_';
SELECT array_join(['hello', 'world']) LIKE '_orld%';

SELECT array_join(['hello', 'world']) LIKE '%he__o%';
SELECT array_join(['hello', 'world']) LIKE '%w__ld%';
SELECT array_join(['hello', 'world']) LIKE '%x%z%';
SELECT array_join(['hello', 'world']) LIKE 'hell_';
SELECT array_join(['hello', 'world']) LIKE '_orld';

SELECT array_join(['hello', 'world']) LIKE 'helloworld';
SELECT array_join(['hello', 'world']) LIKE '%helloworld%';
SELECT array_join(['hello', 'world']) LIKE '%elloworl%';
SELECT array_join(['hello', 'world']) LIKE '%ow%';
SELECT array_join(['hello', 'world']) LIKE '%o%w%';

SELECT array_join(['hello', 'world']) LIKE '%o%';
SELECT array_join(['hello', 'world']) LIKE '%l%';
SELECT array_join(['hello', 'world']) LIKE '%l%o%';
SELECT array_join(['hello', 'world']) LIKE '%o%l%';
