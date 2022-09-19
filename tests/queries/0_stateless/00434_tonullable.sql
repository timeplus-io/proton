SELECT
    toNullable(NULL) AS a,
    toNullable('Hello') AS b,
    toNullable(toNullable(1)) AS c,
    toNullable(materialize(NULL)) AS d,
    toNullable(materialize('Hello')) AS e,
    toNullable(toNullable(materialize(1))) AS f,
    to_type_name(a),
    to_type_name(b),
    to_type_name(c),
    to_type_name(d),
    to_type_name(e),
    to_type_name(f);
