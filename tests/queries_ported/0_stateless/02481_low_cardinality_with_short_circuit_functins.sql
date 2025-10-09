set short_circuit_function_evaluation='force_enable';

select 'if with one LC argument';
select if(0, to_low_cardinality('a'), 'b');
select if(1, to_low_cardinality('a'), 'b');
select if(materialize(0), materialize(to_low_cardinality('a')), materialize('b'));
select if(number % 2, to_low_cardinality('a'), 'b') from numbers(2);
select if(number % 2, materialize(to_low_cardinality('a')), materialize('b')) from numbers(2);

select 'if with LC and NULL arguments';
select if(0, to_low_cardinality('a'), NULL);
select if(1, to_low_cardinality('a'), NULL);
select if(materialize(0), materialize(to_low_cardinality('a')), NULL);
select if(number % 2, to_low_cardinality('a'), NULL) from numbers(2);
select if(number % 2, materialize(to_low_cardinality('a')), NULL) from numbers(2);

select 'if with two LC arguments';
select if(0, to_low_cardinality('a'), to_low_cardinality('b'));
select if(1, to_low_cardinality('a'), to_low_cardinality('b'));
select if(materialize(0), materialize(to_low_cardinality('a')), materialize(to_low_cardinality('b')));
select if(number % 2, to_low_cardinality('a'), to_low_cardinality('b')) from numbers(2);
select if(number % 2, materialize(to_low_cardinality('a')), materialize(to_low_cardinality('a'))) from numbers(2);

select if(number % 2, to_low_cardinality(number), NULL) from numbers(2);
select if(number % 2, to_low_cardinality(number), to_low_cardinality(number + 1)) from numbers(2);

