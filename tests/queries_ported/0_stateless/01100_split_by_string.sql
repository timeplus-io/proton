select split_by_string('ab', 'cdeabcde');
select split_by_string('ab', 'abcdeabcdeab');
select split_by_string('ab', 'ababab');
select split_by_string('ababab', 'ababab');
select split_by_string('', 'abcde');
select split_by_string(', ', x) from (select array_join(['hello, world', 'gbye, bug']) as x);
select split_by_string('ab', '');
select split_by_string('', '');
