select '-- no-tld';
-- even if there is no TLD, 2-nd level by default anyway
-- FIXME: make this behavior optional (so that TLD for host never changed, either empty or something real)
select cut_to_first_significant_subdomain('there-is-no-such-domain');
select cut_to_first_significant_subdomain('foo.there-is-no-such-domain');
select cut_to_first_significant_subdomain('bar.foo.there-is-no-such-domain');
select cut_to_first_significant_subdomain_custom('there-is-no-such-domain', 'public_suffix_list');
select cut_to_first_significant_subdomain_custom('foo.there-is-no-such-domain', 'public_suffix_list');
select cut_to_first_significant_subdomain_custom('bar.foo.there-is-no-such-domain', 'public_suffix_list');
select first_significant_subdomain_custom('bar.foo.there-is-no-such-domain', 'public_suffix_list');

select '-- generic';
select first_significant_subdomain_custom('foo.kernel.biz.ss', 'public_suffix_list'); -- kernel
select cut_to_first_significant_subdomain_custom('foo.kernel.biz.ss', 'public_suffix_list'); -- kernel.biz.ss

select '-- difference';
-- biz.ss is not in the default TLD list, hence:
select cut_to_first_significant_subdomain('foo.kernel.biz.ss'); -- biz.ss
select cut_to_first_significant_subdomain_custom('foo.kernel.biz.ss', 'public_suffix_list'); -- kernel.biz.ss

select '-- 3+level';
select cut_to_first_significant_subdomain_custom('xx.blogspot.co.at', 'public_suffix_list'); -- xx.blogspot.co.at
select first_significant_subdomain_custom('xx.blogspot.co.at', 'public_suffix_list'); -- blogspot
select cut_to_first_significant_subdomain_custom('foo.bar.xx.blogspot.co.at', 'public_suffix_list'); -- xx.blogspot.co.at
select first_significant_subdomain_custom('foo.bar.xx.blogspot.co.at', 'public_suffix_list'); -- blogspot

select '-- url';
select cut_to_first_significant_subdomain_custom('http://foobar.com', 'public_suffix_list');
select cut_to_first_significant_subdomain_custom('http://foobar.com/foo', 'public_suffix_list');
select cut_to_first_significant_subdomain_custom('http://bar.foobar.com/foo', 'public_suffix_list');
select cut_to_first_significant_subdomain_custom('http://xx.blogspot.co.at', 'public_suffix_list');

select '-- www';
select cut_to_first_significant_subdomain_custom_with_www('http://www.foo', 'public_suffix_list');
select cut_to_first_significant_subdomain_custom('http://www.foo', 'public_suffix_list');

select '-- vector';
select cut_to_first_significant_subdomain_custom('http://xx.blogspot.co.at/' || to_string(number), 'public_suffix_list') from numbers(1);
select cut_to_first_significant_subdomain_custom('there-is-no-such-domain' || to_string(number), 'public_suffix_list') from numbers(1);

select '-- no new line';
select cut_to_first_significant_subdomain_custom('foo.bar', 'no_new_line_list');
select cut_to_first_significant_subdomain_custom('a.foo.bar', 'no_new_line_list');
select cut_to_first_significant_subdomain_custom('a.foo.baz', 'no_new_line_list');
