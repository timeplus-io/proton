SELECT * FROM system.macros WHERE macro = 'test';
SELECT get_macro('test');
select isConstant(get_macro('test'));
