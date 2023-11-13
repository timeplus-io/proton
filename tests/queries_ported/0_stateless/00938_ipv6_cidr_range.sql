SELECT 'check invalid params';


SELECT 'tests';

DROP STREAM IF EXISTS ipv6_range;
CREATE STREAM ipv6_range(ip IPv6, cidr uint8) ENGINE = Memory;

INSERT INTO ipv6_range (ip, cidr) VALUES ('2001:0db8:0000:85a3:0000:0000:ac1f:8001', 0), ('2001:0db8:0000:85a3:ffff:ffff:ffff:ffff', 32), ('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff', 16), ('2001:df8:0:85a3::ac1f:8001', 32), ('2001:0db8:85a3:85a3:0000:0000:ac1f:8001', 16), ('0000:0000:0000:0000:0000:0000:0000:0000', 8), ('ffff:0000:0000:0000:0000:0000:0000:0000', 4);

WITH ipv6_cidr_to_range(ipv6_string_to_num('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32) as ip_range SELECT count(*) FROM ipv6_range WHERE ip BETWEEN tuple_element(ip_range, 1) AND tuple_element(ip_range, 2);

WITH ipv6_cidr_to_range(ipv6_string_to_num('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 25) as ip_range SELECT count(*) FROM ipv6_range WHERE ip BETWEEN tuple_element(ip_range, 1) AND tuple_element(ip_range, 2);

WITH ipv6_cidr_to_range(ipv6_string_to_num('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 26) as ip_range SELECT count(*) FROM ipv6_range WHERE ip BETWEEN tuple_element(ip_range, 1) AND tuple_element(ip_range, 2);

WITH ipv6_cidr_to_range(ipv6_string_to_num('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 64) as ip_range SELECT count(*) FROM ipv6_range WHERE ip BETWEEN tuple_element(ip_range, 1) AND tuple_element(ip_range, 2);

WITH ipv6_cidr_to_range(ipv6_string_to_num('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 0) as ip_range SELECT count(*) FROM ipv6_range WHERE ip BETWEEN tuple_element(ip_range, 1) AND tuple_element(ip_range, 2);

SELECT ipv6_num_to_string(ip), cidr, ipv6_cidr_to_range(ip, cidr) FROM ipv6_range;

DROP STREAM ipv6_range;

SELECT ipv6_cidr_to_range(ipv6_string_to_num('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 0);
SELECT ipv6_cidr_to_range(ipv6_string_to_num('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 128);
SELECT ipv6_cidr_to_range(ipv6_string_to_num('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'), 64);
SELECT ipv6_cidr_to_range(ipv6_string_to_num('0000:0000:0000:0000:0000:0000:0000:0000'), 8);
SELECT ipv6_cidr_to_range(ipv6_string_to_num('ffff:0000:0000:0000:0000:0000:0000:0000'), 4);
SELECT ipv6_cidr_to_range(ipv6_string_to_num('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 128) = ipv6_cidr_to_range(ipv6_string_to_num('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 200) ;

SELECT ipv6_cidr_to_range(ipv6_string_to_num('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), to_uint8(128 - number)) FROM numbers(2);
