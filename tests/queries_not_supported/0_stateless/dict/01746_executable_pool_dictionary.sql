-- Tags: no-parallel

SELECT 'executable_pool_simple';

SELECT dictGet('executable_pool_simple', 'a', to_uint64(1));
SELECT dictGet('executable_pool_simple', 'b', to_uint64(1));

SELECT dictGet('executable_pool_simple', 'a', to_uint64(2));
SELECT dictGet('executable_pool_simple', 'b', to_uint64(2));

SELECT 'executable_pool_complex';

SELECT dictGet('executable_pool_complex', 'a', ('First_1', 'Second_1'));
SELECT dictGet('executable_pool_complex', 'b', ('First_1', 'Second_1'));

SELECT dictGet('executable_pool_complex', 'a', ('First_2', 'Second_2'));
SELECT dictGet('executable_pool_complex', 'b', ('First_2', 'Second_2'));
