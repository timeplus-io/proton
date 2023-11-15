SELECT 'IPv4 functions';

SELECT ipv4_string_to_num_or_default('test');
SELECT ipv4_string_to_num_or_null('test');

SELECT ipv4_string_to_num('127.0.0.1');
SELECT ipv4_string_to_num_or_default('127.0.0.1');
SELECT ipv4_string_to_num_or_null('127.0.0.1');

SELECT '--';

SELECT to_ipv4_or_default('test');
SELECT to_ipv4_or_null('test');

SELECT to_ipv4('127.0.0.1');
SELECT to_ipv4_or_default('127.0.0.1');
SELECT to_ipv4_or_null('127.0.0.1');

SELECT '--';

SELECT to_ipv4(to_ipv6('::ffff:1.2.3.4'));
SELECT to_ipv4_or_default(to_ipv6('::ffff:1.2.3.4'));
-- SELECT to_ipv4_or_default(to_ipv6('::afff:1.2.3.4'));

SELECT '--';

SELECT cast('127.0.0.1' , 'IPv4');

SELECT '--';

SET cast_ipv4_ipv6_default_on_conversion_error = 1;

SELECT ipv4_string_to_num('test');
SELECT to_ipv4('test');
SELECT ipv4_string_to_num('');
SELECT to_ipv4('');
SELECT cast('test' , 'IPv4');
SELECT cast('' , 'IPv4');

SET cast_ipv4_ipv6_default_on_conversion_error = 0;

SELECT 'IPv6 functions';

SELECT ipv6_string_to_num_or_default('test');
SELECT ipv6_string_to_num_or_null('test');

SELECT ipv6_string_to_num('::ffff:127.0.0.1');
SELECT ipv6_string_to_num_or_default('::ffff:127.0.0.1');
SELECT ipv6_string_to_num_or_null('::ffff:127.0.0.1');

SELECT '--';

SELECT to_ipv6_or_default('test');
SELECT to_ipv6_or_null('test');

SELECT to_ipv6('::ffff:127.0.0.1');
SELECT to_ipv6_or_default('::ffff:127.0.0.1');
SELECT to_ipv6_or_null('::ffff:127.0.0.1');

SELECT to_ipv6_or_default('::.1.2.3');
SELECT to_ipv6_or_null('::.1.2.3');

SELECT count() FROM numbers_mt(100000000) WHERE NOT ignore(to_ipv4_or_zero(random_string(8)));

SELECT '--';

SELECT cast('::ffff:127.0.0.1', 'IPv6');

SELECT '--';

SET cast_ipv4_ipv6_default_on_conversion_error = 1;

SELECT ipv6_string_to_num('test');
SELECT to_ipv6('test');
SELECT ipv6_string_to_num('');
SELECT to_ipv6('');
SELECT cast('test' , 'IPv6');
SELECT cast('' , 'IPv6');

SELECT '--';

SET cast_ipv4_ipv6_default_on_conversion_error = 0;

SELECT to_fixed_string('::1', 5) as value, cast(value, 'IPv6'), to_ipv6(value);
SELECT to_fixed_string('', 16) as value, cast(value, 'IPv6');
SELECT to_fixed_string('', 16) as value, to_ipv6(value);
