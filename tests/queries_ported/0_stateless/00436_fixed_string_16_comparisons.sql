SELECT
    a, b, a = b, a != b, a < b, a > b, a <= b, a >= b,
    to_fixed_string(a, 16) AS fa, to_fixed_string(b, 16) AS fb, fa = fb, fa != fb, fa < fb, fa > fb, fa <= fb, fa >= fb
FROM
(
    SELECT 'aaaaaaaaaaaaaaaa' AS a
    UNION ALL SELECT 'aaaaaaaaaaaaaaab'
    UNION ALL SELECT 'aaaaaaaaaaaaaaac'
    UNION ALL SELECT 'baaaaaaaaaaaaaaa'
    UNION ALL SELECT 'baaaaaaaaaaaaaab'
    UNION ALL SELECT 'baaaaaaaaaaaaaac'
    UNION ALL SELECT 'aaaaaaaabaaaaaaa'
    UNION ALL SELECT 'aaaaaaabaaaaaaaa'
    UNION ALL SELECT 'aaaaaaacaaaaaaaa'
) as js1
CROSS JOIN
(
    SELECT 'aaaaaaaaaaaaaaaa' AS b
    UNION ALL SELECT 'aaaaaaaaaaaaaaab'
    UNION ALL SELECT 'aaaaaaaaaaaaaaac'
    UNION ALL SELECT 'baaaaaaaaaaaaaaa'
    UNION ALL SELECT 'baaaaaaaaaaaaaab'
    UNION ALL SELECT 'baaaaaaaaaaaaaac'
    UNION ALL SELECT 'aaaaaaaabaaaaaaa'
    UNION ALL SELECT 'aaaaaaabaaaaaaaa'
    UNION ALL SELECT 'aaaaaaacaaaaaaaa'
) as js2
ORDER BY a, b;


SELECT
    to_fixed_string(a, 16) AS a,
    to_fixed_string('aaaaaaaaaaaaaaaa', 16) AS b1,
    to_fixed_string('aaaaaaaaaaaaaaab', 16) AS b2,
    to_fixed_string('aaaaaaaaaaaaaaac', 16) AS b3,
    to_fixed_string('baaaaaaaaaaaaaaa', 16) AS b4,
    to_fixed_string('baaaaaaaaaaaaaab', 16) AS b5,
    to_fixed_string('baaaaaaaaaaaaaac', 16) AS b6,
    to_fixed_string('aaaaaaaabaaaaaaa', 16) AS b7,
    to_fixed_string('aaaaaaabaaaaaaaa', 16) AS b8,
    to_fixed_string('aaaaaaacaaaaaaaa', 16) AS b9,
    a = b1, a != b1, a < b1, a > b1, a <= b1, a >= b1,
    a = b2, a != b2, a < b2, a > b2, a <= b2, a >= b2,
    a = b3, a != b3, a < b3, a > b3, a <= b3, a >= b3,
    a = b4, a != b4, a < b4, a > b4, a <= b4, a >= b4,
    a = b5, a != b5, a < b5, a > b5, a <= b5, a >= b5,
    a = b6, a != b6, a < b6, a > b6, a <= b6, a >= b6,
    a = b7, a != b7, a < b7, a > b7, a <= b7, a >= b7,
    a = b8, a != b8, a < b8, a > b8, a <= b8, a >= b8,
    a = b9, a != b9, a < b9, a > b9, a <= b9, a >= b9
FROM
(
    SELECT 'aaaaaaaaaaaaaaaa' AS a
    UNION ALL SELECT 'aaaaaaaaaaaaaaab'
    UNION ALL SELECT 'aaaaaaaaaaaaaaac'
    UNION ALL SELECT 'baaaaaaaaaaaaaaa'
    UNION ALL SELECT 'baaaaaaaaaaaaaab'
    UNION ALL SELECT 'baaaaaaaaaaaaaac'
    UNION ALL SELECT 'aaaaaaaabaaaaaaa'
    UNION ALL SELECT 'aaaaaaabaaaaaaaa'
    UNION ALL SELECT 'aaaaaaacaaaaaaaa'
)
ORDER BY a;
