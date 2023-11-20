set query_mode = 'table';
DROP STREAM IF EXISTS test_table_ipv4;
CREATE STREAM test_table_ipv4
(
    ip string,
    ipv4 ipv4
) ENGINE=Memory;


SET input_format_ipv4_default_on_conversion_error = 1;

INSERT INTO test_table_ipv4(ip, ipv4) VALUES ('1.1.1.1', '1.1.1.1'), ('', '');

SELECT ip, ipv4 FROM test_table_ipv4;

SET input_format_ipv4_default_on_conversion_error = 0;

DROP STREAM test_table_ipv4;

DROP STREAM IF EXISTS test_table_ipv4_materialized;
CREATE STREAM test_table_ipv4_materialized
(
    ip string,
    ipv6 ipv4 MATERIALIZED to_ipv4(ip)
) ENGINE=Memory;


SET input_format_ipv4_default_on_conversion_error = 1;


SET cast_ipv4_ipv6_default_on_conversion_error = 1;

INSERT INTO test_table_ipv4_materialized(ip) VALUES ('1.1.1.1'), ('');
SELECT ip, ipv6 FROM test_table_ipv4_materialized;

SET input_format_ipv4_default_on_conversion_error = 0;
SET cast_ipv4_ipv6_default_on_conversion_error = 0;

DROP STREAM test_table_ipv4_materialized;

DROP STREAM IF EXISTS test_table_ipv6;
CREATE STREAM test_table_ipv6
(
    ip string,
    ipv6 ipv6
) ENGINE=Memory;


SET input_format_ipv6_default_on_conversion_error = 1;

INSERT INTO test_table_ipv6 (ip, ipv6) VALUES ('fe80::9801:43ff:fe1f:7690', 'fe80::9801:43ff:fe1f:7690'), ('1.1.1.1', '1.1.1.1'), ('', '');
SELECT ip, ipv6 FROM test_table_ipv6;

SET input_format_ipv6_default_on_conversion_error = 0;

DROP STREAM test_table_ipv6;

DROP STREAM IF EXISTS test_table_ipv6_materialized;
CREATE STREAM test_table_ipv6_materialized
(
    ip string,
    ipv6 ipv6 MATERIALIZED to_ipv6(ip)
) ENGINE = Memory;


SET input_format_ipv6_default_on_conversion_error = 1;


SET cast_ipv4_ipv6_default_on_conversion_error = 1;

INSERT INTO test_table_ipv6_materialized(ip) VALUES ('fe80::9801:43ff:fe1f:7690'), ('1.1.1.1'), ('');
SELECT ip, ipv6 FROM test_table_ipv6_materialized;

SET input_format_ipv6_default_on_conversion_error = 0;
SET cast_ipv4_ipv6_default_on_conversion_error = 0;

DROP STREAM test_table_ipv6_materialized;
