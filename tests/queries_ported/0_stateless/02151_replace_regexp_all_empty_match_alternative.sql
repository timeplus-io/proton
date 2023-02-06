SELECT replace_regexp_all(',,1,,', '^[,]*|[,]*$', '');
SELECT replace_regexp_all(',,1', '^[,]*|[,]*$', '');
SELECT replace_regexp_all('1,,', '^[,]*|[,]*$', '');

SELECT replace_regexp_all(materialize(',,1,,'), '^[,]*|[,]*$', '');
SELECT replace_regexp_all(materialize(',,1'), '^[,]*|[,]*$', '');
SELECT replace_regexp_all(materialize('1,,'), '^[,]*|[,]*$', '');

SELECT replace_regexp_all('a', 'z*', '') == 'a';
SELECT replace_regexp_all('aa', 'z*', '') == 'aa';
SELECT replace_regexp_all('aaq', 'z*', '') == 'aaq';
SELECT replace_regexp_all('aazq', 'z*', '') == 'aaq';
SELECT replace_regexp_all('aazzq', 'z*', '') == 'aaq';
SELECT replace_regexp_all('aazzqa', 'z*', '') == 'aaqa';

SELECT replace_regexp_all(materialize('a'), 'z*', '') == 'a';
SELECT replace_regexp_all(materialize('aa'), 'z*', '') == 'aa';
SELECT replace_regexp_all(materialize('aaq'), 'z*', '') == 'aaq';
SELECT replace_regexp_all(materialize('aazq'), 'z*', '') == 'aaq';
SELECT replace_regexp_all(materialize('aazzq'), 'z*', '') == 'aaq';
SELECT replace_regexp_all(materialize('aazzqa'), 'z*', '') == 'aaqa';
