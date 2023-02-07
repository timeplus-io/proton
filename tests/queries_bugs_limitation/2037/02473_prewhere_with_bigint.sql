DROP STREAM IF EXISTS prewhere_int128;
DROP STREAM IF EXISTS prewhere_int256;
DROP STREAM IF EXISTS prewhere_uint128;
DROP STREAM IF EXISTS prewhere_uint256;

CREATE STREAM prewhere_int128 (a int128) ENGINE=MergeTree ORDER BY a;
INSERT INTO prewhere_int128 VALUES (1);
SELECT a FROM prewhere_int128 PREWHERE a; -- { serverError 59 }
DROP STREAM prewhere_int128;

CREATE STREAM prewhere_int256 (a int256) ENGINE=MergeTree ORDER BY a;
INSERT INTO prewhere_int256 VALUES (1);
SELECT a FROM prewhere_int256 PREWHERE a; -- { serverError 59 }
DROP STREAM prewhere_int256;

CREATE STREAM prewhere_uint128 (a uint128) ENGINE=MergeTree ORDER BY a;
INSERT INTO prewhere_uint128 VALUES (1);
SELECT a FROM prewhere_uint128 PREWHERE a; -- { serverError 59 }
DROP STREAM prewhere_uint128;

CREATE STREAM prewhere_uint256 (a uint256) ENGINE=MergeTree ORDER BY a;
INSERT INTO prewhere_uint256 VALUES (1);
SELECT a FROM prewhere_uint256 PREWHERE a; -- { serverError 59 }
DROP STREAM prewhere_uint256;
