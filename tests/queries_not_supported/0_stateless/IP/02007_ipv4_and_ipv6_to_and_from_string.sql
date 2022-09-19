SELECT CAST('127.0.0.1' as IPv4) as v, to_type_name(v);
SELECT CAST(toIPv4('127.0.0.1') as string) as v, to_type_name(v);

SELECT CAST('2001:0db8:0000:85a3:0000:0000:ac1f:8001' as IPv6) as v, to_type_name(v);
SELECT CAST(toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001') as string) as v, to_type_name(v);

SELECT toIPv4('hello') as v, to_type_name(v);
SELECT toIPv6('hello') as v, to_type_name(v);

SELECT CAST('hello' as IPv4) as v, to_type_name(v); -- { serverError CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING }
SELECT CAST('hello' as IPv6) as v, to_type_name(v); -- { serverError CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING }

SELECT CAST('1.1.1.1' as IPv6) as v, to_type_name(v); -- { serverError CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING }
