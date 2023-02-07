select to_datetime(0, '/abc'); -- { serverError POCO_EXCEPTION }
select to_datetime(0, './abc'); -- { serverError POCO_EXCEPTION }
select to_datetime(0, '../abc'); -- { serverError POCO_EXCEPTION }
select to_datetime(0, '~/abc'); -- { serverError POCO_EXCEPTION }
select to_datetime(0, 'abc/../../cba'); -- { serverError POCO_EXCEPTION }

