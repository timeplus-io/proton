SELECT array_join(split_by_char(',', to_low_cardinality('a,b,c')));

