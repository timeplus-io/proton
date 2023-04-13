SELECT
    materialize(to_low_cardinality('')) AS lc,
    to_type_name(lc)
WHERE lc = default_value_of_argument_type(lc)
