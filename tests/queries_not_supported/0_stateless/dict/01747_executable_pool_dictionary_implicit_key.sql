-- Tags: no-parallel

SELECT 'executable_pool_simple_implicit_key';

SELECT dictGet('executable_pool_simple_implicit_key', 'a', to_uint64(1));
SELECT dictGet('executable_pool_simple_implicit_key', 'b', to_uint64(1));

SELECT dictGet('executable_pool_simple_implicit_key', 'a', to_uint64(2));
SELECT dictGet('executable_pool_simple_implicit_key', 'b', to_uint64(2));

SELECT 'executable_pool_complex_implicit_key';

SELECT dictGet('executable_pool_complex_implicit_key', 'a', ('First_1', 'Second_1'));
SELECT dictGet('executable_pool_complex_implicit_key', 'b', ('First_1', 'Second_1'));

SELECT dictGet('executable_pool_complex_implicit_key', 'a', ('First_2', 'Second_2'));
SELECT dictGet('executable_pool_complex_implicit_key', 'b', ('First_2', 'Second_2'));
