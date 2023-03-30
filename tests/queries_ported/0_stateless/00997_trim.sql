WITH
    '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' AS x,
    replace_regexp_all(x, '.', ' ') AS spaces,
    concat(substring(spaces, 1, rand(1) % 62), substring(x, 1, rand(2) % 62), substring(spaces, 1, rand(3) % 62)) AS s,
    trim_left(s) AS sl,
    trim_right(s) AS sr,
    trim_both(s) AS t,
     replace_regexp_one(s, '^ +', '') AS slr,
     replace_regexp_one(s, ' +$', '') AS srr,
     replace_regexp_one(s, '^ *(.*?) *$', '\\1') AS tr
SELECT
    replace_all(s, ' ', '_'),
    replace_all(sl, ' ', '_'),
    replace_all(slr, ' ', '_'),
    replace_all(sr, ' ', '_'),
    replace_all(srr, ' ', '_'),
    replace_all(t, ' ', '_'),
    replace_all(tr, ' ', '_')
FROM numbers(100000)
WHERE NOT ((sl = slr) AND (sr = srr) AND (t = tr))
