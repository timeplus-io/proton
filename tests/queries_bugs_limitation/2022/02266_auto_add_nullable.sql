SET allow_suspicious_low_cardinality_types = 1;
DROP STREAM IF EXISTS 02266_auto_add_nullable;

CREATE STREAM 02266_auto_add_nullable
(
    val0 int8 DEFAULT NULL,
    val1 nullable(int8) DEFAULT NULL,
    val2 uint8 DEFAULT NUll,
    val3 string DEFAULT null,
    val4 low_cardinality(int8) DEFAULT NULL,
    val5 low_cardinality(nullable(int8)) DEFAULT NULL
)
ENGINE = MergeTree order by tuple();

DESCRIBE STREAM 02266_auto_add_nullable;

DROP STREAM IF EXISTS 02266_auto_add_nullable;