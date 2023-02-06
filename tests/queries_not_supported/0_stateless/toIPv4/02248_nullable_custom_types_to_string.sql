select to_string(to_nullable(true));
select to_string(CAST(NULL, 'nullable(bool)'));
select to_string(to_nullable(toIPv4('0.0.0.0')));
select to_string(CAST(NULL, 'nullable(ipv4)'));
select to_string(to_nullable(toIPv6('::ffff:127.0.0.1')));
select to_string(CAST(NULL, 'nullable(ipv6)'));
