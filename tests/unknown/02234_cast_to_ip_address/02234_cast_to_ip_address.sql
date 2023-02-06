SELECT 'ipv4 functions';

SELECT ipv4stringToNum('test'); --{serverError CANNOT_PARSE_IPV4}
SELECT ipv4stringToNumOrDefault('test');
SELECT ipv4stringToNum_or_null('test');

SELECT ipv4stringToNum('127.0.0.1');
SELECT ipv4stringToNumOrDefault('127.0.0.1');
SELECT ipv4stringToNum_or_null('127.0.0.1');

SELECT '--';

SELECT toIPv4('test'); --{serverError CANNOT_PARSE_IPV4}
SELECT toIPv4OrDefault('test');
SELECT toIPv4_or_null('test');

SELECT toIPv4('127.0.0.1');
SELECT toIPv4OrDefault('127.0.0.1');
SELECT toIPv4_or_null('127.0.0.1');

SELECT '--';

SELECT cast('test' , 'ipv4'); --{serverError CANNOT_PARSE_IPV4}
SELECT cast('127.0.0.1' , 'ipv4');

SELECT '--';

SET cast_ipv4_ipv6_default_on_conversion_error = 1;

SELECT ipv4stringToNum('test');
SELECT toIPv4('test');
SELECT ipv4stringToNum('');
SELECT toIPv4('');
SELECT cast('test' , 'ipv4');
SELECT cast('' , 'ipv4');

SET cast_ipv4_ipv6_default_on_conversion_error = 0;

SELECT 'ipv6 functions';

SELECT ipv6stringToNum('test'); --{serverError CANNOT_PARSE_IPV6}
SELECT ipv6stringToNumOrDefault('test');
SELECT ipv6stringToNum_or_null('test');

SELECT ipv6stringToNum('::ffff:127.0.0.1');
SELECT ipv6stringToNumOrDefault('::ffff:127.0.0.1');
SELECT ipv6stringToNum_or_null('::ffff:127.0.0.1');

SELECT '--';

SELECT toIPv6('test'); --{serverError CANNOT_PARSE_IPV6}
SELECT toIPv6OrDefault('test');
SELECT toIPv6_or_null('test');

SELECT toIPv6('::ffff:127.0.0.1');
SELECT toIPv6OrDefault('::ffff:127.0.0.1');
SELECT toIPv6_or_null('::ffff:127.0.0.1');

SELECT '--';

SELECT cast('test' , 'ipv6'); --{serverError CANNOT_PARSE_IPV6}
SELECT cast('::ffff:127.0.0.1', 'ipv6');

SELECT '--';

SET cast_ipv4_ipv6_default_on_conversion_error = 1;

SELECT ipv6stringToNum('test');
SELECT toIPv6('test');
SELECT ipv6stringToNum('');
SELECT toIPv6('');
SELECT cast('test' , 'ipv6');
SELECT cast('' , 'ipv6');

SELECT '--';

SET cast_ipv4_ipv6_default_on_conversion_error = 0;

SELECT to_fixed_string('::1', 5) as value, cast(value, 'ipv6'), toIPv6(value);
SELECT to_fixed_string('', 16) as value, cast(value, 'ipv6');
SELECT to_fixed_string('', 16) as value, toIPv6(value);
