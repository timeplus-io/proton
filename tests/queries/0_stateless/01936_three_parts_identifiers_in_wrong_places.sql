SELECT dictGet(t.nest.a, concat(currentDatabase(), '.dict.dict'), 's', number) FROM numbers(5); -- { serverError 47 }

SELECT dictGetFloat64(t.b.s, 'database_for_dict.dict1', dictGetFloat64('Ta\0', to_uint64('databas\0_for_dict.dict1databas\0_for_dict.dict1', dictGetFloat64('', '', to_uint64(1048577), to_date(NULL)), NULL), to_date(dictGetFloat64(257, 'database_for_dict.dict1database_for_dict.dict1', '', to_uint64(NULL), 2, to_date(NULL)), '2019-05-2\0')), NULL, to_uint64(dictGetFloat64('', '', to_uint64(-9223372036854775808), to_date(NULL)), NULL)); -- { serverError 47 }

SELECT NULL AND (2147483648 AND NULL) AND -2147483647, toUUID(((1048576 AND NULL) AND (2147483647 AND 257 AND NULL AND -2147483649) AND NULL) IN (test_01103.t1_distr.id), '00000000-e1fe-11e\0-bb8f\0853d60c00749'), stringToH3('89184926cc3ffff89184926cc3ffff89184926cc3ffff89184926cc3ffff89184926cc3ffff89184926cc3ffff89184926cc3ffff89184926cc3ffff'); -- { serverError 47 }

SELECT 'still alive';
