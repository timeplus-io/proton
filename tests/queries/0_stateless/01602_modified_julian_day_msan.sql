SELECT tryBase64Decode(( SELECT countSubstrings(toModifiedJulianDayOrNull('\0'), '') ) AS n, ( SELECT regionIn('l. ') ) AS srocpnuv); -- { serverError 43 }
SELECT countSubstrings(toModifiedJulianDayOrNull('\0'), ''); -- { serverError 43 }
SELECT countSubstrings(to_int32OrNull('123qwe123'), ''); -- { serverError 43 }
SELECT 'Ok.';
