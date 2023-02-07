SELECT to_type_name([to_low_cardinality('1'), to_low_cardinality('2')]);
SELECT to_type_name([materialize(to_low_cardinality('1')), to_low_cardinality('2')]);
SELECT to_type_name([to_low_cardinality('1'), materialize(to_low_cardinality('2'))]);
SELECT to_type_name([materialize(to_low_cardinality('1')), materialize(to_low_cardinality('2'))]);

SELECT to_type_name([to_low_cardinality('1'), '2']);
SELECT to_type_name([materialize(to_low_cardinality('1')), '2']);
SELECT to_type_name([to_low_cardinality('1'), materialize('2')]);
SELECT to_type_name([materialize(to_low_cardinality('1')), materialize('2')]);

SELECT to_type_name(map(to_low_cardinality('1'), to_low_cardinality('2')));
SELECT to_type_name(map(materialize(to_low_cardinality('1')), to_low_cardinality('2')));
SELECT to_type_name(map(to_low_cardinality('1'), materialize(to_low_cardinality('2'))));
SELECT to_type_name(map(materialize(to_low_cardinality('1')), materialize(to_low_cardinality('2'))));
