select '{ key: fn, value: concat }' == concat('{ key: ', to_fixed_string('fn', 2), ', value: ', 'concat', ' }');

select concat('a', 'b') == 'ab';
select concat('a', materialize('b')) == 'ab';
select concat(materialize('a'), 'b') == 'ab';
select concat(materialize('a'), materialize('b')) == 'ab';

select concat('a', to_fixed_string('b', 1)) == 'ab';
select concat('a', materialize(to_fixed_string('b', 1))) == 'ab';
select concat(materialize('a'), to_fixed_string('b', 1)) == 'ab';
select concat(materialize('a'), materialize(to_fixed_string('b', 1))) == 'ab';

select concat(to_fixed_string('a', 1), 'b') == 'ab';
select concat(to_fixed_string('a', 1), materialize('b')) == 'ab';
select concat(materialize(to_fixed_string('a', 1)), 'b') == 'ab';
select concat(materialize(to_fixed_string('a', 1)), materialize('b')) == 'ab';

select concat(to_fixed_string('a', 1), to_fixed_string('b', 1)) == 'ab';
select concat(to_fixed_string('a', 1), materialize(to_fixed_string('b', 1))) == 'ab';
select concat(materialize(to_fixed_string('a', 1)), to_fixed_string('b', 1)) == 'ab';
select concat(materialize(to_fixed_string('a', 1)), materialize(to_fixed_string('b', 1))) == 'ab';

select concat('a', 'b') == 'ab' from system.numbers limit 5;
select concat('a', materialize('b')) == 'ab' from system.numbers limit 5;
select concat(materialize('a'), 'b') == 'ab' from system.numbers limit 5;
select concat(materialize('a'), materialize('b')) == 'ab' from system.numbers limit 5;

select concat('a', to_fixed_string('b', 1)) == 'ab' from system.numbers limit 5;
select concat('a', materialize(to_fixed_string('b', 1))) == 'ab' from system.numbers limit 5;
select concat(materialize('a'), to_fixed_string('b', 1)) == 'ab' from system.numbers limit 5;
select concat(materialize('a'), materialize(to_fixed_string('b', 1))) == 'ab' from system.numbers limit 5;

select concat(to_fixed_string('a', 1), 'b') == 'ab' from system.numbers limit 5;
select concat(to_fixed_string('a', 1), materialize('b')) == 'ab' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), 'b') == 'ab' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), materialize('b')) == 'ab' from system.numbers limit 5;

select concat(to_fixed_string('a', 1), to_fixed_string('b', 1)) == 'ab' from system.numbers limit 5;
select concat(to_fixed_string('a', 1), materialize(to_fixed_string('b', 1))) == 'ab' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), to_fixed_string('b', 1)) == 'ab' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), materialize(to_fixed_string('b', 1))) == 'ab' from system.numbers limit 5;

select concat('a', 'b', 'c') == 'abc';
select concat('a', 'b', materialize('c')) == 'abc';
select concat('a', materialize('b'), 'c') == 'abc';
select concat('a', materialize('b'), materialize('c')) == 'abc';
select concat(materialize('a'), 'b', 'c') == 'abc';
select concat(materialize('a'), 'b', materialize('c')) == 'abc';
select concat(materialize('a'), materialize('b'), 'c') == 'abc';
select concat(materialize('a'), materialize('b'), materialize('c')) == 'abc';

select concat('a', 'b', to_fixed_string('c', 1)) == 'abc';
select concat('a', 'b', materialize(to_fixed_string('c', 1))) == 'abc';
select concat('a', materialize('b'), to_fixed_string('c', 1)) == 'abc';
select concat('a', materialize('b'), materialize(to_fixed_string('c', 1))) == 'abc';
select concat(materialize('a'), 'b', to_fixed_string('c', 1)) == 'abc';
select concat(materialize('a'), 'b', materialize(to_fixed_string('c', 1))) == 'abc';
select concat(materialize('a'), materialize('b'), to_fixed_string('c', 1)) == 'abc';
select concat(materialize('a'), materialize('b'), materialize(to_fixed_string('c', 1))) == 'abc';

select concat('a', to_fixed_string('b', 1), 'c') == 'abc';
select concat('a', to_fixed_string('b', 1), materialize('c')) == 'abc';
select concat('a', materialize(to_fixed_string('b', 1)), 'c') == 'abc';
select concat('a', materialize(to_fixed_string('b', 1)), materialize('c')) == 'abc';
select concat(materialize('a'), to_fixed_string('b', 1), 'c') == 'abc';
select concat(materialize('a'), to_fixed_string('b', 1), materialize('c')) == 'abc';
select concat(materialize('a'), materialize(to_fixed_string('b', 1)), 'c') == 'abc';
select concat(materialize('a'), materialize(to_fixed_string('b', 1)), materialize('c')) == 'abc';

select concat('a', to_fixed_string('b', 1), to_fixed_string('c', 1)) == 'abc';
select concat('a', to_fixed_string('b', 1), materialize(to_fixed_string('c', 1))) == 'abc';
select concat('a', materialize(to_fixed_string('b', 1)), to_fixed_string('c', 1)) == 'abc';
select concat('a', materialize(to_fixed_string('b', 1)), materialize(to_fixed_string('c', 1))) == 'abc';
select concat(materialize('a'), to_fixed_string('b', 1), to_fixed_string('c', 1)) == 'abc';
select concat(materialize('a'), to_fixed_string('b', 1), materialize(to_fixed_string('c', 1))) == 'abc';
select concat(materialize('a'), materialize(to_fixed_string('b', 1)), to_fixed_string('c', 1)) == 'abc';
select concat(materialize('a'), materialize(to_fixed_string('b', 1)), materialize(to_fixed_string('c', 1))) == 'abc';

select concat(to_fixed_string('a', 1), 'b', 'c') == 'abc';
select concat(to_fixed_string('a', 1), 'b', materialize('c')) == 'abc';
select concat(to_fixed_string('a', 1), materialize('b'), 'c') == 'abc';
select concat(to_fixed_string('a', 1), materialize('b'), materialize('c')) == 'abc';
select concat(materialize(to_fixed_string('a', 1)), 'b', 'c') == 'abc';
select concat(materialize(to_fixed_string('a', 1)), 'b', materialize('c')) == 'abc';
select concat(materialize(to_fixed_string('a', 1)), materialize('b'), 'c') == 'abc';
select concat(materialize(to_fixed_string('a', 1)), materialize('b'), materialize('c')) == 'abc';

select concat(to_fixed_string('a', 1), 'b', to_fixed_string('c', 1)) == 'abc';
select concat(to_fixed_string('a', 1), 'b', materialize(to_fixed_string('c', 1))) == 'abc';
select concat(to_fixed_string('a', 1), materialize('b'), to_fixed_string('c', 1)) == 'abc';
select concat(to_fixed_string('a', 1), materialize('b'), materialize(to_fixed_string('c', 1))) == 'abc';
select concat(materialize(to_fixed_string('a', 1)), 'b', to_fixed_string('c', 1)) == 'abc';
select concat(materialize(to_fixed_string('a', 1)), 'b', materialize(to_fixed_string('c', 1))) == 'abc';
select concat(materialize(to_fixed_string('a', 1)), materialize('b'), to_fixed_string('c', 1)) == 'abc';
select concat(materialize(to_fixed_string('a', 1)), materialize('b'), materialize(to_fixed_string('c', 1))) == 'abc';

select concat(to_fixed_string('a', 1), to_fixed_string('b', 1), 'c') == 'abc';
select concat(to_fixed_string('a', 1), to_fixed_string('b', 1), materialize('c')) == 'abc';
select concat(to_fixed_string('a', 1), materialize(to_fixed_string('b', 1)), 'c') == 'abc';
select concat(to_fixed_string('a', 1), materialize(to_fixed_string('b', 1)), materialize('c')) == 'abc';
select concat(materialize(to_fixed_string('a', 1)), to_fixed_string('b', 1), 'c') == 'abc';
select concat(materialize(to_fixed_string('a', 1)), to_fixed_string('b', 1), materialize('c')) == 'abc';
select concat(materialize(to_fixed_string('a', 1)), materialize(to_fixed_string('b', 1)), 'c') == 'abc';
select concat(materialize(to_fixed_string('a', 1)), materialize(to_fixed_string('b', 1)), materialize('c')) == 'abc';

select concat(to_fixed_string('a', 1), to_fixed_string('b', 1), to_fixed_string('c', 1)) == 'abc';
select concat(to_fixed_string('a', 1), to_fixed_string('b', 1), materialize(to_fixed_string('c', 1))) == 'abc';
select concat(to_fixed_string('a', 1), materialize(to_fixed_string('b', 1)), to_fixed_string('c', 1)) == 'abc';
select concat(to_fixed_string('a', 1), materialize(to_fixed_string('b', 1)), materialize(to_fixed_string('c', 1))) == 'abc';
select concat(materialize(to_fixed_string('a', 1)), to_fixed_string('b', 1), to_fixed_string('c', 1)) == 'abc';
select concat(materialize(to_fixed_string('a', 1)), to_fixed_string('b', 1), materialize(to_fixed_string('c', 1))) == 'abc';
select concat(materialize(to_fixed_string('a', 1)), materialize(to_fixed_string('b', 1)), to_fixed_string('c', 1)) == 'abc';
select concat(materialize(to_fixed_string('a', 1)), materialize(to_fixed_string('b', 1)), materialize(to_fixed_string('c', 1))) == 'abc';

select concat('a', 'b', 'c') == 'abc' from system.numbers limit 5;
select concat('a', 'b', materialize('c')) == 'abc' from system.numbers limit 5;
select concat('a', materialize('b'), 'c') == 'abc' from system.numbers limit 5;
select concat('a', materialize('b'), materialize('c')) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), 'b', 'c') == 'abc' from system.numbers limit 5;
select concat(materialize('a'), 'b', materialize('c')) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), materialize('b'), 'c') == 'abc' from system.numbers limit 5;
select concat(materialize('a'), materialize('b'), materialize('c')) == 'abc' from system.numbers limit 5;

select concat('a', 'b', to_fixed_string('c', 1)) == 'abc' from system.numbers limit 5;
select concat('a', 'b', materialize(to_fixed_string('c', 1))) == 'abc' from system.numbers limit 5;
select concat('a', materialize('b'), to_fixed_string('c', 1)) == 'abc' from system.numbers limit 5;
select concat('a', materialize('b'), materialize(to_fixed_string('c', 1))) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), 'b', to_fixed_string('c', 1)) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), 'b', materialize(to_fixed_string('c', 1))) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), materialize('b'), to_fixed_string('c', 1)) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), materialize('b'), materialize(to_fixed_string('c', 1))) == 'abc' from system.numbers limit 5;

select concat('a', to_fixed_string('b', 1), 'c') == 'abc' from system.numbers limit 5;
select concat('a', to_fixed_string('b', 1), materialize('c')) == 'abc' from system.numbers limit 5;
select concat('a', materialize(to_fixed_string('b', 1)), 'c') == 'abc' from system.numbers limit 5;
select concat('a', materialize(to_fixed_string('b', 1)), materialize('c')) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), to_fixed_string('b', 1), 'c') == 'abc' from system.numbers limit 5;
select concat(materialize('a'), to_fixed_string('b', 1), materialize('c')) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), materialize(to_fixed_string('b', 1)), 'c') == 'abc' from system.numbers limit 5;
select concat(materialize('a'), materialize(to_fixed_string('b', 1)), materialize('c')) == 'abc' from system.numbers limit 5;

select concat('a', to_fixed_string('b', 1), to_fixed_string('c', 1)) == 'abc' from system.numbers limit 5;
select concat('a', to_fixed_string('b', 1), materialize(to_fixed_string('c', 1))) == 'abc' from system.numbers limit 5;
select concat('a', materialize(to_fixed_string('b', 1)), to_fixed_string('c', 1)) == 'abc' from system.numbers limit 5;
select concat('a', materialize(to_fixed_string('b', 1)), materialize(to_fixed_string('c', 1))) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), to_fixed_string('b', 1), to_fixed_string('c', 1)) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), to_fixed_string('b', 1), materialize(to_fixed_string('c', 1))) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), materialize(to_fixed_string('b', 1)), to_fixed_string('c', 1)) == 'abc' from system.numbers limit 5;
select concat(materialize('a'), materialize(to_fixed_string('b', 1)), materialize(to_fixed_string('c', 1))) == 'abc' from system.numbers limit 5;

select concat(to_fixed_string('a', 1), 'b', 'c') == 'abc' from system.numbers limit 5;
select concat(to_fixed_string('a', 1), 'b', materialize('c')) == 'abc' from system.numbers limit 5;
select concat(to_fixed_string('a', 1), materialize('b'), 'c') == 'abc' from system.numbers limit 5;
select concat(to_fixed_string('a', 1), materialize('b'), materialize('c')) == 'abc' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), 'b', 'c') == 'abc' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), 'b', materialize('c')) == 'abc' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), materialize('b'), 'c') == 'abc' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), materialize('b'), materialize('c')) == 'abc' from system.numbers limit 5;

select concat(to_fixed_string('a', 1), 'b', to_fixed_string('c', 1)) == 'abc' from system.numbers limit 5;
select concat(to_fixed_string('a', 1), 'b', materialize(to_fixed_string('c', 1))) == 'abc' from system.numbers limit 5;
select concat(to_fixed_string('a', 1), materialize('b'), to_fixed_string('c', 1)) == 'abc' from system.numbers limit 5;
select concat(to_fixed_string('a', 1), materialize('b'), materialize(to_fixed_string('c', 1))) == 'abc' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), 'b', to_fixed_string('c', 1)) == 'abc' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), 'b', materialize(to_fixed_string('c', 1))) == 'abc' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), materialize('b'), to_fixed_string('c', 1)) == 'abc' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), materialize('b'), materialize(to_fixed_string('c', 1))) == 'abc' from system.numbers limit 5;

select concat(to_fixed_string('a', 1), to_fixed_string('b', 1), 'c') == 'abc' from system.numbers limit 5;
select concat(to_fixed_string('a', 1), to_fixed_string('b', 1), materialize('c')) == 'abc' from system.numbers limit 5;
select concat(to_fixed_string('a', 1), materialize(to_fixed_string('b', 1)), 'c') == 'abc' from system.numbers limit 5;
select concat(to_fixed_string('a', 1), materialize(to_fixed_string('b', 1)), materialize('c')) == 'abc' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), to_fixed_string('b', 1), 'c') == 'abc' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), to_fixed_string('b', 1), materialize('c')) == 'abc' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), materialize(to_fixed_string('b', 1)), 'c') == 'abc' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), materialize(to_fixed_string('b', 1)), materialize('c')) == 'abc' from system.numbers limit 5;

select concat(to_fixed_string('a', 1), to_fixed_string('b', 1), to_fixed_string('c', 1)) == 'abc' from system.numbers limit 5;
select concat(to_fixed_string('a', 1), to_fixed_string('b', 1), materialize(to_fixed_string('c', 1))) == 'abc' from system.numbers limit 5;
select concat(to_fixed_string('a', 1), materialize(to_fixed_string('b', 1)), to_fixed_string('c', 1)) == 'abc' from system.numbers limit 5;
select concat(to_fixed_string('a', 1), materialize(to_fixed_string('b', 1)), materialize(to_fixed_string('c', 1))) == 'abc' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), to_fixed_string('b', 1), to_fixed_string('c', 1)) == 'abc' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), to_fixed_string('b', 1), materialize(to_fixed_string('c', 1))) == 'abc' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), materialize(to_fixed_string('b', 1)), to_fixed_string('c', 1)) == 'abc' from system.numbers limit 5;
select concat(materialize(to_fixed_string('a', 1)), materialize(to_fixed_string('b', 1)), materialize(to_fixed_string('c', 1))) == 'abc' from system.numbers limit 5;
