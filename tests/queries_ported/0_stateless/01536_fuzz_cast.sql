SET cast_keep_nullable = 0;
SELECT CAST(array_join([NULL, '', '', NULL, '', NULL, '01.02.2017 03:04\005GMT', '', NULL, '01/02/2017 03:04:05 MSK01/02/\0017 03:04:05 MSK', '', NULL, '03/04/201903/04/201903/04/\001903/04/2019']), 'enum8(\'a\' = 1, \'b\' = 2)') AS x; -- { serverError 349 }
