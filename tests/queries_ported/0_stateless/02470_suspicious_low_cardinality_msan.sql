DROP STREAM IF EXISTS alias_2__fuzz_25;
SET allow_suspicious_low_cardinality_types = 1;
CREATE STREAM alias_2__fuzz_25 (`dt` low_cardinality(Date), `col` DateTime, `col2` nullable(int256), `colAlias0` nullable(DateTime64(3)) ALIAS col, `colAlias3` nullable(int32) ALIAS col3 + colAlias0, `colAlias1` low_cardinality(uint16) ALIAS colAlias0 + col2, `colAlias2` low_cardinality(int32) ALIAS colAlias0 + colAlias1, `col3` nullable(uint8)) ENGINE = MergeTree ORDER BY dt;
insert into alias_2__fuzz_25 (dt, col, col2, col3) values ('2020-02-01', 1, 2, 3);
SELECT colAlias0, colAlias2, colAlias3 FROM alias_2__fuzz_25; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP STREAM alias_2__fuzz_25;
