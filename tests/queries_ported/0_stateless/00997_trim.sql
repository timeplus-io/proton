WITH
    '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' AS x,
    replaceRegexpAll(x, '.', ' ') AS spaces,
    concat(substring(spaces, 1, rand(1) % 62), substring(x, 1, rand(2) % 62), substring(spaces, 1, rand(3) % 62)) AS s,
    trim_left(s) AS sl,
    trim_right(s) AS sr,
    trim_both(s) AS t,
     replace_regexp_one(s, '^ +', '') AS slr,
     replace_regexp_one(s, ' +$', '') AS srr,
     replace_regexp_one(s, '^ *(.*?) *$', '\\1') AS tr
SELECT
    replaceAll(s, ' ', '_'),
    replaceAll(sl, ' ', '_'),
    replaceAll(slr, ' ', '_'),
    replaceAll(sr, ' ', '_'),
    replaceAll(srr, ' ', '_'),
    replaceAll(t, ' ', '_'),
    replaceAll(tr, ' ', '_')
FROM numbers(100000)
WHERE NOT ((sl = slr) AND (sr = srr) AND (t = tr))
