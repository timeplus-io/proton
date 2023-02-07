-- Tags: no-backward-compatibility-check
-- TODO: remove no-backward-compatibility-check after new 22.6 release

SET cast_ipv4_ipv6_default_on_conversion_error = 1;

DROP STREAM IF EXISTS ipv4_test;
CREATE STREAM ipv4_test
(
    id uint64,
    value string
) ENGINE=MergeTree ORDER BY id;

ALTER STREAM ipv4_test MODIFY COLUMN value ipv4 DEFAULT '';

SET cast_ipv4_ipv6_default_on_conversion_error = 0;

DETACH STREAM ipv4_test;
ATTACH STREAM ipv4_test;

SET cast_ipv4_ipv6_default_on_conversion_error = 1;

DROP STREAM ipv4_test;

DROP STREAM IF EXISTS ipv6_test;
CREATE STREAM ipv6_test
(
    id uint64,
    value string
) ENGINE=MergeTree ORDER BY id;

ALTER STREAM ipv6_test MODIFY COLUMN value ipv6 DEFAULT '';

SET cast_ipv4_ipv6_default_on_conversion_error = 0;

DETACH STREAM ipv6_test;
ATTACH STREAM ipv6_test;

SET cast_ipv4_ipv6_default_on_conversion_error = 1;

SELECT * FROM ipv6_test;

DROP STREAM ipv6_test;
