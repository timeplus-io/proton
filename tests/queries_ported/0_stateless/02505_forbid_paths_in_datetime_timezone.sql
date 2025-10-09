select to_datetime(0, '/abc'); -- { serverError BAD_ARGUMENTS }
select to_datetime(0, './abc'); -- { serverError BAD_ARGUMENTS }
select to_datetime(0, '../abc'); -- { serverError BAD_ARGUMENTS }
select to_datetime(0, '~/abc'); -- { serverError BAD_ARGUMENTS }
select to_datetime(0, 'abc/../../cba'); -- { serverError BAD_ARGUMENTS }

