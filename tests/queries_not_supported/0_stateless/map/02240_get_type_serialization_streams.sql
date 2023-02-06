select get_type_serialization_streams('array(int8)');
select get_type_serialization_streams('map(string, int64)');
select get_type_serialization_streams('Tuple(string, int64, float64)');
select get_type_serialization_streams('low_cardinality(string)');
select get_type_serialization_streams('nullable(string)');
select get_type_serialization_streams([1,2,3]);
select get_type_serialization_streams(map('a', 1, 'b', 2));
select get_type_serialization_streams(tuple('a', 1, 'b', 2));
