select cast(cast(NULL, 'nullable(string)'), 'nullable(bool)');
select cast(cast(NULL, 'nullable(string)'), 'nullable(ipv4)');
select cast(cast(NULL, 'nullable(string)'), 'nullable(ipv6)');

select to_bool(cast(NULL, 'nullable(string)'));
select to_ipv4(cast(NULL, 'nullable(string)'));
select ipv4_string_to_num(cast(NULL, 'nullable(string)'));
select to_ipv6(cast(NULL, 'nullable(string)'));
select ipv6_string_to_num(cast(NULL, 'nullable(string)'));

select cast(number % 2 ? 'true' : NULL, 'nullable(bool)') from numbers(2);
select cast(number % 2 ? '0.0.0.0' : NULL, 'nullable(ipv4)') from numbers(2);
select cast(number % 2 ? '0000:0000:0000:0000:0000:0000:0000:0000' : NULL, 'nullable(ipv6)') from numbers(2);

set cast_keep_nullable = 1;
select to_bool(number % 2 ? 'true' : NULL) from numbers(2);
select to_ipv4(number % 2 ? '0.0.0.0' : NULL) from numbers(2);
select to_ipv4_or_default(number % 2 ? '' : NULL) from numbers(2);
select to_ipv4_or_null(number % 2 ? '' : NULL) from numbers(2);
select ipv4_string_to_num(number % 2 ? '0.0.0.0' : NULL) from numbers(2);
select to_ipv6(number % 2 ? '0000:0000:0000:0000:0000:0000:0000:0000' : NULL) from numbers(2);
select to_ipv6_or_default(number % 2 ? '' : NULL) from numbers(2);
select to_ipv6_or_null(number % 2 ? '' : NULL) from numbers(2);
select ipv6_string_to_num(number % 2 ? '0000:0000:0000:0000:0000:0000:0000:0000' : NULL) from numbers(2);
