SET allow_suspicious_low_cardinality_types = 1;

SELECT to_type_name(tuple(to_low_cardinality('1'), to_low_cardinality(1)));
SELECT to_type_name(tuple(materialize(to_low_cardinality('1')), to_low_cardinality(1)));
SELECT to_type_name(tuple(to_low_cardinality('1'), materialize(to_low_cardinality(1))));
SELECT to_type_name(tuple(materialize(to_low_cardinality('1')), materialize(to_low_cardinality(1))));
