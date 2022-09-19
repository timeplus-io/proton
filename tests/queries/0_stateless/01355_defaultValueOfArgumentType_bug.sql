SELECT
    materialize(toLowCardinality('')) AS lc,
    to_type_name(lc)
WHERE lc = defaultValueOfArgumentType(lc)
