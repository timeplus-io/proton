SELECT tryBase64Decode(( SELECT count_substrings(to_modified_julian_day_or_null('\0'), '') ) AS n, ( SELECT regionIn('l. ') ) AS srocpnuv); -- { serverError 43 }
SELECT count_substrings(to_modified_julian_day_or_null('\0'), ''); -- { serverError 43 }
SELECT count_substrings(to_int32_or_null('123qwe123'), ''); -- { serverError 43 }
SELECT 'Ok.';
