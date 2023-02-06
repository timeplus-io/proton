select split_by_regexp('\\d+', x) from (select array_join(['a1ba5ba8b', 'a11ba5689ba891011b']) as x);
select split_by_regexp('', 'abcde');
select split_by_regexp('<[^<>]*>', x) from (select array_join(['<h1>hello<h2>world</h2></h1>', 'gbye<split>bug']) as x);
select split_by_regexp('ab', '');
select split_by_regexp('', '');
