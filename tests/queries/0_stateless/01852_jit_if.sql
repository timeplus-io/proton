SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

DROP STREAM IF EXISTS test_jit_nonnull;
create stream test_jit_nonnull (value uint8) ;
INSERT INTO test_jit_nonnull VALUES (0), (1);

SELECT 'test_jit_nonnull';
SELECT value, multiIf(value = 1, 2, value, 1, 0), if (value, 1, 0) FROM test_jit_nonnull;

DROP STREAM IF EXISTS test_jit_nullable;
create stream test_jit_nullable (value Nullable(uint8)) ;
INSERT INTO test_jit_nullable VALUES (0), (1), (NULL);

SELECT 'test_jit_nullable';
SELECT value, multiIf(value = 1, 2, value, 1, 0), if (value, 1, 0) FROM test_jit_nullable;

DROP STREAM test_jit_nonnull;
DROP STREAM test_jit_nullable;
