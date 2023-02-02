select 'basic';
select count_matches('', 'foo');
select count_matches('foo', '');
-- simply stop if zero bytes was processed
select count_matches('foo', '[f]{0}');
-- but this is ok
select count_matches('foo', '[f]{0}foo');

select 'case sensitive';
select count_matches('foobarfoo', 'foo');
select count_matches('foobarfoo', 'foo.*');
select count_matches('oooo', 'oo');
select count_matches(concat(to_string(number), 'foofoo'), 'foo') from numbers(2);
select count_matches('foobarbazfoobarbaz', 'foo(bar)(?:baz|)');
select count_matches('foo.com bar.com baz.com bam.com', '([^. ]+)\.([^. ]+)');
select count_matches('foo.com@foo.com bar.com@foo.com baz.com@foo.com bam.com@foo.com', '([^. ]+)\.([^. ]+)@([^. ]+)\.([^. ]+)');

select 'case insensitive';
select count_matches_case_insensitive('foobarfoo', 'FOo');
select count_matches_case_insensitive('foobarfoo', 'FOo.*');
select count_matches_case_insensitive('oooo', 'Oo');
select count_matches_case_insensitive(concat(to_string(number), 'Foofoo'), 'foo') from numbers(2);
select count_matches_case_insensitive('foOBarBAZfoobarbaz', 'foo(bar)(?:baz|)');
select count_matches_case_insensitive('foo.com BAR.COM baz.com bam.com', '([^. ]+)\.([^. ]+)');
select count_matches_case_insensitive('foo.com@foo.com bar.com@foo.com BAZ.com@foo.com bam.com@foo.com', '([^. ]+)\.([^. ]+)@([^. ]+)\.([^. ]+)');

select 'errors';
select count_matches(1, 'foo') from numbers(1); -- { serverError 43; }
select count_matches('foobarfoo', to_string(number)) from numbers(1); -- { serverError 44; }
