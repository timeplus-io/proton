DROP STREAM IF EXISTS bftest;
CREATE STREAM bftest (
    k int64,
    y array(int64) DEFAULT x,
    x array(int64),
    index ix1(x) TYPE bloom_filter GRANULARITY 3
)
Engine=MergeTree
ORDER BY k;

INSERT INTO bftest (k, x) SELECT number, array_map(i->rand64()%565656, range(10)) FROM numbers(1000);

-- index is not used, but query should still work
SELECT count() FROM bftest WHERE hasAll(x, materialize([1,2,3])) FORMAT Null;

-- verify the expression in WHERE works on non-index col the same way as on index cols
SELECT count() FROM bftest WHERE hasAll(y, [NULL,-42]) FORMAT Null;
SELECT count() FROM bftest WHERE hasAll(y, [0,NULL]) FORMAT Null;
SELECT count() FROM bftest WHERE hasAll(y, [[123], -42]) FORMAT Null; -- { serverError 386 }
SELECT count() FROM bftest WHERE hasAll(y, [to_decimal32(123, 3), 2]) FORMAT Null; -- different, doesn't fail

SET force_data_skipping_indices='ix1';
SELECT count() FROM bftest WHERE has (x, 42) and has(x, -42) FORMAT Null;
SELECT count() FROM bftest WHERE hasAll(x, [42,-42]) FORMAT Null;
SELECT count() FROM bftest WHERE hasAll(x, []) FORMAT Null;
SELECT count() FROM bftest WHERE hasAll(x, [1]) FORMAT Null;

-- can't use bloom_filter with `hasAll` on non-constant arguments (just like `has`)
SELECT count() FROM bftest WHERE hasAll(x, materialize([1,2,3])) FORMAT Null; -- { serverError 277 }

-- NULLs are not Ok
SELECT count() FROM bftest WHERE hasAll(x, [NULL,-42]) FORMAT Null; -- { serverError 277 }
SELECT count() FROM bftest WHERE hasAll(x, [0,NULL]) FORMAT Null; -- { serverError 277 }

-- non-compatible types
SELECT count() FROM bftest WHERE hasAll(x, [[123], -42]) FORMAT Null; -- { serverError 386 }
SELECT count() FROM bftest WHERE hasAll(x, [to_decimal32(123, 3), 2]) FORMAT Null; -- { serverError 277 }

-- Bug discovered by AST fuzzier (fixed, shouldn't crash).
SELECT 1 FROM bftest WHERE has(x, -0.) OR 0. FORMAT Null;
SELECT count() FROM bftest WHERE hasAll(x, [0, 1]) OR 0. FORMAT Null;
