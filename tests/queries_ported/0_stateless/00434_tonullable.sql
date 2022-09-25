SELECT
    to_nullable(NULL) AS a,
    to_nullable('Hello') AS b,
    to_nullable(to_nullable(1)) AS c,
    to_nullable(materialize(NULL)) AS d,
    to_nullable(materialize('Hello')) AS e,
    to_nullable(to_nullable(materialize(1))) AS f,
    to_type_name(a),
    to_type_name(b),
    to_type_name(c),
    to_type_name(d),
    to_type_name(e),
    to_type_name(f);
