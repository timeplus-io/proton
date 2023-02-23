select concatWithSeparator('|', 'a', 'b') == 'a|b';
select concatWithSeparator('|', 'a', materialize('b')) == 'a|b';
select concatWithSeparator('|', materialize('a'), 'b') == 'a|b';
select concatWithSeparator('|', materialize('a'), materialize('b')) == 'a|b';

select concatWithSeparator('|', 'a', to_fixed_string('b', 1)) == 'a|b';
select concatWithSeparator('|', 'a', materialize(to_fixed_string('b', 1))) == 'a|b';
select concatWithSeparator('|', materialize('a'), to_fixed_string('b', 1)) == 'a|b';
select concatWithSeparator('|', materialize('a'), materialize(to_fixed_string('b', 1))) == 'a|b';

select concatWithSeparator('|', to_fixed_string('a', 1), 'b') == 'a|b';
select concatWithSeparator('|', to_fixed_string('a', 1), materialize('b')) == 'a|b';
select concatWithSeparator('|', materialize(to_fixed_string('a', 1)), 'b') == 'a|b';
select concatWithSeparator('|', materialize(to_fixed_string('a', 1)), materialize('b')) == 'a|b';

select concatWithSeparator('|', to_fixed_string('a', 1), to_fixed_string('b', 1)) == 'a|b';
select concatWithSeparator('|', to_fixed_string('a', 1), materialize(to_fixed_string('b', 1))) == 'a|b';
select concatWithSeparator('|', materialize(to_fixed_string('a', 1)), to_fixed_string('b', 1)) == 'a|b';
select concatWithSeparator('|', materialize(to_fixed_string('a', 1)), materialize(to_fixed_string('b', 1))) == 'a|b';

select concatWithSeparator(null, 'a', 'b') == null;
select concatWithSeparator('1', null, 'b') == null;
select concatWithSeparator('1', 'a', null) == null;

select concatWithSeparator(materialize('|'), 'a', 'b'); -- { serverError 44 }
select concatWithSeparator();                           -- { serverError 42 }
select concatWithSeparator('|', 'a', 100);              -- { serverError 43 }
