select CAST(CAST(NULL, 'nullable(string)'), 'nullable(bool)');
select CAST(CAST(NULL, 'nullable(string)'), 'nullable(ipv4)');
select CAST(CAST(NULL, 'nullable(string)'), 'nullable(ipv6)');

select toBool(CAST(NULL, 'nullable(string)'));
select toIPv4(CAST(NULL, 'nullable(string)'));
select ipv4stringToNum(CAST(NULL, 'nullable(string)'));
select toIPv6(CAST(NULL, 'nullable(string)'));
select ipv6stringToNum(CAST(NULL, 'nullable(string)'));

select CAST(number % 2 ? 'true' : NULL, 'nullable(bool)') from numbers(2);
select CAST(number % 2 ? '0.0.0.0' : NULL, 'nullable(ipv4)') from numbers(2);
select CAST(number % 2 ? '0000:0000:0000:0000:0000:0000:0000:0000' : NULL, 'nullable(ipv6)') from numbers(2);

set cast_keep_nullable = 1;
select toBool(number % 2 ? 'true' : NULL) from numbers(2);
select toIPv4(number % 2 ? '0.0.0.0' : NULL) from numbers(2);
select toIPv4OrDefault(number % 2 ? '' : NULL) from numbers(2);
select toIPv4_or_null(number % 2 ? '' : NULL) from numbers(2);
select ipv4stringToNum(number % 2 ? '0.0.0.0' : NULL) from numbers(2);
select toIPv6(number % 2 ? '0000:0000:0000:0000:0000:0000:0000:0000' : NULL) from numbers(2);
select toIPv6OrDefault(number % 2 ? '' : NULL) from numbers(2);
select toIPv6_or_null(number % 2 ? '' : NULL) from numbers(2);
select ipv6stringToNum(number % 2 ? '0000:0000:0000:0000:0000:0000:0000:0000' : NULL) from numbers(2);
