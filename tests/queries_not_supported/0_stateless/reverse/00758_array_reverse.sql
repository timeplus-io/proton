SELECT reverse([NULL, '\0']);
SELECT reverse([NULL, 123, NULL]);
SELECT reverse([to_fixed_string('Hello', 5), NULL]);
SELECT reverse(['Hello', 'world']);
SELECT reverse(['Hello', NULL, 'world']);
SELECT reverse([NULL, NULL, NULL]);
SELECT reverse([[], [''], [' ']]);
SELECT reverse([[], [''], [NULL]]);
SELECT reverse([(1, 'Hello', []), (nan, 'World', [NULL])]);
SELECT reverse(NULL);
SELECT reverse([]);
SELECT reverse([[[[]]]]);

 
SELECT '[RE7', ( SELECT '\0' ) AS riwwq, ( SELECT reverse([( SELECT bit_test_all(NULL) ) , ( SELECT '\0' ) AS ddfweeuy]) ) AS xuvv, '', ( SELECT * FROM file() ) AS wqgdswyc, ( SELECT * FROM file() ); -- { serverError 42 }
