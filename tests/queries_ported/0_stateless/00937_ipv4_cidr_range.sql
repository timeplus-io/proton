SELECT 'tests';

DROP STREAM IF EXISTS ipv4_range;
create stream ipv4_range(ip ipv4, cidr uint8);

INSERT INTO ipv4_range (ip, cidr) VALUES (to_ipv4('192.168.5.2'), 0), (to_ipv4('192.168.5.20'), 32), (to_ipv4('255.255.255.255'), 16), (to_ipv4('192.142.32.2'), 32), (to_ipv4('192.172.5.2'), 16), (to_ipv4('0.0.0.0'), 8), (to_ipv4('255.0.0.0'), 4);

SELECT sleep(3);

WITH ipv4_cidr_to_range(to_ipv4('192.168.0.0'), 8) as ip_range SELECT count(*) FROM table(ipv4_range) WHERE ip BETWEEN tuple_element(ip_range, 1) AND tuple_element(ip_range, 2);

WITH ipv4_cidr_to_range(to_ipv4('192.168.0.0'), 13) as ip_range SELECT count(*) FROM table(ipv4_range) WHERE ip BETWEEN tuple_element(ip_range, 1) AND tuple_element(ip_range, 2);

WITH ipv4_cidr_to_range(to_ipv4('192.168.0.0'), 16) as ip_range SELECT count(*) FROM table(ipv4_range) WHERE ip BETWEEN tuple_element(ip_range, 1) AND tuple_element(ip_range, 2);

WITH ipv4_cidr_to_range(to_ipv4('192.168.0.0'), 0) as ip_range SELECT count(*) FROM table(ipv4_range) WHERE ip BETWEEN tuple_element(ip_range, 1) AND tuple_element(ip_range, 2);

WITH ipv4_cidr_to_range(ip, cidr) as ip_range SELECT ip, cidr, ipv4_num_to_string(tuple_element(ip_range, 1)), ip_range FROM table(ipv4_range);

DROP STREAM ipv4_range;

SELECT ipv4_cidr_to_range(to_ipv4('192.168.5.2'), 0);
SELECT ipv4_cidr_to_range(to_ipv4('255.255.255.255'), 8);
SELECT ipv4_cidr_to_range(to_ipv4('192.168.5.2'), 32);
SELECT ipv4_cidr_to_range(to_ipv4('0.0.0.0'), 8);
SELECT ipv4_cidr_to_range(to_ipv4('255.0.0.0'), 4);

SELECT ipv4_cidr_to_range(to_ipv4('255.0.0.0'), to_uint8(4 + number)) FROM numbers(2);
