select splitByString('ab', 'cdeabcde');
select splitByString('ab', 'abcdeabcdeab');
select splitByString('ab', 'ababab');
select splitByString('ababab', 'ababab');
select splitByString('', 'abcde');
select splitByString(', ', x) from (select array_join(['hello, world', 'gbye, bug']) x);
select splitByString('ab', '');
select splitByString('', '');
